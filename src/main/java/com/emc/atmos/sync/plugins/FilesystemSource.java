/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.atmos.sync.plugins;

import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.sync.util.AtmosMetadata;
import com.emc.atmos.sync.util.CountingInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

/**
 * The filesystem source reads data from a file or directory.
 * @author cwikj
 */
public class FilesystemSource extends MultithreadedCrawlSource {
	private static final Logger l4j = Logger.getLogger(FilesystemSource.class);
	
	public static final String IGNORE_META_OPT = "ignore-meta-dir";
	public static final String IGNORE_META_DESC = "Ignores any metadata in the " + AtmosMetadata.META_DIR + " directory";
	
	public static final String ABSOLUTE_PATH_OPT = "use-absolute-path";
	public static final String ABSOLUTE_PATH_DESC = "Uses the absolute path to the file when storing it instead of the relative path from the source dir.";
	
	public static final String DELETE_OLDER_OPT = "delete-older-than";
	public static final String DELETE_OLDER_DESC = "when --delete is used, add this option to only delete files that have been modified more than <delete-age> milliseconds ago";
	public static final String DELETE_OLDER_ARG_NAME = "delete-age";
	
	public static final String DELETE_CHECK_OPT = "delete-check-script";
	public static final String DELETE_CHECK_DESC = "when --delete is used, add this option to execute an external script to check whether a file should be deleted.  If the process exits with return code zero, the file is safe to delete.";
	public static final String DELETE_CHECK_ARG_NAME = "path-to-check-script";

	protected File source;
	protected boolean recursive;
    protected boolean useAbsolutePath = false;
	private boolean ignoreMeta = false;
	protected boolean delete = false;
	private long deleteOlderThan = 0;
	private File deleteCheckScript;
    protected int bufferSize = CommonOptions.DEFAULT_BUFFER_SIZE;
	
	private MimetypesFileTypeMap mimeMap;

	public FilesystemSource() {
		mimeMap = new MimetypesFileTypeMap();
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SourcePlugin#run()
	 */
	@Override
	public void run() {
		running = true;
		initQueue();
		
		// Enqueue the root task.
		ReadFileTask rootTask = new ReadFileTask(source);
		submitCrawlTask(rootTask);
		
		runQueue();
		
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SourcePlugin#terminate()
	 */
	@Override
	public void terminate() {
		running = false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		opts.addOption(OptionBuilder.withDescription(IGNORE_META_DESC)
				.withLongOpt(IGNORE_META_OPT).create());
		opts.addOption(OptionBuilder.withDescription(ABSOLUTE_PATH_DESC)
				.withLongOpt(ABSOLUTE_PATH_OPT).create());
		opts.addOption(OptionBuilder.withLongOpt(DELETE_OLDER_OPT)
				.withDescription(DELETE_OLDER_DESC)
				.hasArg().withArgName(DELETE_OLDER_ARG_NAME).create());
		opts.addOption(OptionBuilder.withLongOpt(DELETE_CHECK_OPT)
				.withDescription(DELETE_CHECK_DESC)
				.hasArg().withArgName(DELETE_CHECK_ARG_NAME).create());
		addOptions(opts);
		
		return opts;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
        super.parseOptions(line);

		String sourceOption = line.getOptionValue(CommonOptions.SOURCE_OPTION);
		if(sourceOption == null) {
			return false;
		}
		if(sourceOption.startsWith("file://")) {
			URI u;
			try {
				u = new URI(sourceOption);
			} catch (URISyntaxException e) {
				throw new RuntimeException("Failed to parse URI: " + sourceOption + ": " + e.getMessage(), e);
			}
			source = new File(u);
			if(!source.exists()) {
				throw new RuntimeException("The source " + source + " does not exist");
			}
			
			if(line.hasOption(CommonOptions.RECURSIVE_OPTION)) {
				recursive = true;
			}
			if(line.hasOption(IGNORE_META_OPT)) {
				ignoreMeta = true;
			}
			if(line.hasOption(CommonOptions.DELETE_OPTION)) {
				delete = true;
			}
			if(line.hasOption(DELETE_OLDER_OPT)) {
				deleteOlderThan = Long.parseLong(line.getOptionValue(DELETE_OLDER_OPT));
			}
			if(line.hasOption(DELETE_CHECK_OPT)) {
				deleteCheckScript = new File(line.getOptionValue(DELETE_CHECK_OPT));
				if(!deleteCheckScript.exists()) {
					throw new RuntimeException("Delete check script " + 
							deleteCheckScript + " does not exist");
				}
			}
            if (line.hasOption(CommonOptions.IO_BUFFER_SIZE_OPTION)) {
                bufferSize = Integer.parseInt(line.getOptionValue(CommonOptions.IO_BUFFER_SIZE_OPTION));
            }
			
			return true;
		}
		return false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "Filesystem Source";
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "The filesystem source reads data from a file or directory.  " + 
				"It is triggered by setting the source to a valid File URL:\n" +
				"file://<path>, e.g. file:///home/user/myfiles\n" +
				"If the URL refers to a file, only that file will be " + 
				"transferred.  If a directory is specified, the contents of " +
				"the directory will be transferred.  If the --recursive" + 
				"flag is set, the subdirectories will also be recursively " +
				"transferred.  By default, any Atmos metadata files inside " +
				AtmosMetadata.META_DIR + " directories will be assigned to their " +
				"corresponding files; use --" + IGNORE_META_OPT +
				" to ignore the metadata directory.";
	}
		
	public class ReadFileTask implements Runnable {
		private File f;
		private Map<String, String> extraMetadata;

		public ReadFileTask(File f) {
			this.f = f;
		}

		@Override
		public void run() {
			FileSyncObject fso;
			try {
				fso = createFileSyncObject(f);
				if(extraMetadata != null) {
					for(String key : extraMetadata.keySet()) {
						String value = extraMetadata.get(key);
						fso.getMetadata().getMetadata().put(key,
								new Metadata(key, value, false));
					}
				}
			} catch(Exception e) {
				l4j.error("Error creating FileSyncObject: " + e, e);
				return;
			}
			try {
				getNext().filter(fso);
				
				if(delete) {
					// Try to lock the file first.  If this fails, the file is
					// probably open for write somewhere.
					// Note that on a mac, you can apparently delete files that
					// someone else has open for writing, and can lock files 
					// too.
					if(f.isDirectory()) {
						// Just try and delete
						if(!f.delete()) {
							LogMF.warn(l4j, "Failed to delete {0}", f);
						}						
					} else {
						boolean tryDelete = true;
						if(deleteOlderThan > 0) {
							if(System.currentTimeMillis() - f.lastModified() < deleteOlderThan) {
								LogMF.debug(l4j, 
										"Not deleting {0}; it is not at least {1} ms old", 
										f, deleteOlderThan);
								tryDelete = false;
							}
						}
						if(deleteCheckScript != null) {
							String[] args = new String[] { 
									deleteCheckScript.getAbsolutePath(), 
									f.getAbsolutePath()
							};
							try {
								l4j.debug("Delete check: " + Arrays.asList(args));
								Process p = Runtime.getRuntime().exec(args);
								while(true) {
									try {
										int exitCode = p.exitValue();
										
										if(exitCode == 0) {
											LogMF.debug(l4j, 
													"Delete check OK, exit code {0}", 
													exitCode);
										} else {
											LogMF.info(l4j, 
													"Delete check failed, exit code {0}.  Not deleting file.", 
													exitCode);
											tryDelete = false;
										}
										break;
									} catch(IllegalThreadStateException e) {
										// Ignore.
									}
								}
							} catch(IOException e) {
								LogMF.info(l4j, 
										"Error executing delete check script: {0}.  Not deleting file.", 
										e.toString());
								tryDelete = false;
							}
						}
						RandomAccessFile raf = null;
						if(tryDelete) {
							try {
								raf = new RandomAccessFile(f, "rw");
								FileChannel fc = raf.getChannel();
								FileLock flock = fc.lock();
								// If we got here, we should be good.
								flock.release();
								if(!f.delete()) {
									LogMF.warn(l4j, "Failed to delete {0}", f);
								}
							} catch(IOException e) {
								LogMF.info(l4j, 
										"File {0} not deleted, it appears to be open: {1}", 
										f, e.getMessage());
							} finally {
								if(raf != null) {
									raf.close();
								}
							}
						}
					}

					
				}
				
				complete(fso);
			} catch(Throwable t) {
				failed(fso, t);
				return;
			}
			try {
				if(f.isDirectory()) {
					LogMF.debug(l4j, ">Crawling {0}", f);
					for(File child : f.listFiles()) {
                        if (child.isFile()) {
                            // File objects go into the bounded
                            // transfer queue.  Note that adding to the transfer
                            // queue might block if it's full.
                            LogMF.debug(l4j, "+transfer {0}", child);
                            submitTransferTask(new ReadFileTask(child));
                        } else if (recursive && !child.getName().equals(AtmosMetadata.META_DIR)) {
                            // Directories that need crawling go into the crawler
                            // queue.  Note that adding to the transfer
                            // queue might block if it's full.
                            LogMF.debug(l4j, "+crawl {0}", child);
                            submitCrawlTask(new ReadFileTask(child));
                        }
					}
					LogMF.debug(l4j, "<Done Crawling {0}", f);
				}
			} catch(Exception e) { 
				l4j.error("Error enumerating directory: " + f, e);
				failed(fso, e);
			}
			
		}

		/**
		 * @return the extraMetadata
		 */
		public Map<String, String> getExtraMetadata() {
			return extraMetadata;
		}

		/**
		 * @param extraMetadata the extraMetadata to set
		 */
		public void setExtraMetadata(Map<String, String> extraMetadata) {
			this.extraMetadata = extraMetadata;
		}		
		
	}

    /**
     * Override to provide a different FileSyncObject implementation
     */
    protected FileSyncObject createFileSyncObject(File f) {
        return new FileSyncObject(f);
    }
	
	public class FileSyncObject extends SyncObject {
		protected File f;
		protected CountingInputStream in;
		protected String relativePath;
		
		@Override
		public boolean isDirectory() {
			return f.isDirectory();
		}
		
		public FileSyncObject(File f) {
			this.f = f;
			setSize(f.length());
			setSourceURI(f.toURI());
			
			File metaFile = AtmosMetadata.getMetaFile(f);
			if(metaFile.exists() && !ignoreMeta) {
				try {
					setMetadata(AtmosMetadata.fromFile(metaFile));
				} catch (Exception e) {
					LogMF.warn(l4j, "Could not read metadata from {0}: {1}", metaFile, e.getMessage());
				}
			} else {
				// Default is empty, but we'll throw in the mime type.
				AtmosMetadata am = new AtmosMetadata();
				am.setContentType(mimeMap.getContentType(f));
				am.setMtime(new Date(f.lastModified()));
				setMetadata(am);
			}
			
            relativePath = f.getAbsolutePath();
            if(!useAbsolutePath && relativePath.startsWith(source.getAbsolutePath())) {
                relativePath = relativePath.substring(source.getAbsolutePath().length());
			}
            if(File.separatorChar == '\\') {
                relativePath = relativePath.replace('\\', '/');
            }
            if(relativePath.startsWith("/")) {
				relativePath = relativePath.substring(1);
			}
			if(f.isDirectory() && !relativePath.endsWith("/") && relativePath.length()>0) {
				relativePath += "/"; // Dirs must end with a slash (except for root);
			}
		}


		@Override
		public synchronized InputStream getInputStream() {
			if(f.isDirectory()) {
				return null;
			}
			if(in == null) {
				try {
					in = new CountingInputStream(new BufferedInputStream(new FileInputStream(f), bufferSize));
				} catch (FileNotFoundException e) {
					throw new RuntimeException("Could not open file:" + f, e);
				}
			}
			
			return in;
		}
		
		public long getBytesRead() {
			if(in != null) {
				return in.getBytesRead();
			} else {
				return 0;
			}
		}

		@Override
		public String getRelativePath() {
			return relativePath;
		}
		
	}

	/**
	 * @return the source
	 */
	public File getSource() {
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(File source) {
		this.source = source;
	}

	/**
	 * @return the recursive
	 */
	public boolean isRecursive() {
		return recursive;
	}

	/**
	 * @param recursive the recursive to set
	 */
	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}

	/**
	 * @return the useAbsolutePath
	 */
	public boolean isUseAbsolutePath() {
		return useAbsolutePath;
	}

	/**
	 * @param useAbsolutePath the useAbsolutePath to set
	 */
	public void setUseAbsolutePath(boolean useAbsolutePath) {
		this.useAbsolutePath = useAbsolutePath;
	}

	/**
	 * @return the ignoreMeta
	 */
	public boolean isIgnoreMeta() {
		return ignoreMeta;
	}

	/**
	 * @param ignoreMeta the ignoreMeta to set
	 */
	public void setIgnoreMeta(boolean ignoreMeta) {
		this.ignoreMeta = ignoreMeta;
	}

	/**
	 * @return the mimeMap
	 */
	public MimetypesFileTypeMap getMimeMap() {
		return mimeMap;
	}

	/**
	 * @param mimeMap the mimeMap to set
	 */
	public void setMimeMap(MimetypesFileTypeMap mimeMap) {
		this.mimeMap = mimeMap;
	}

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
