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
package com.emc.vipr.sync.source;

import com.emc.vipr.sync.CommonOptions;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.FilesystemUtil;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.ReadOnlyIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;

/**
 * The filesystem source reads data from a file or directory.
 */
public class FilesystemSource extends SyncSource<FilesystemSource.FileSyncObject> {
    private static final Logger l4j = Logger.getLogger(FilesystemSource.class);

    protected static final String FILE_PREFIX = "file://";

    public static final String ABSOLUTE_PATH_OPT = "use-absolute-path";
    public static final String ABSOLUTE_PATH_DESC = "Uses the absolute path to the file when storing it instead of the relative path from the source dir.";

    public static final String DELETE_OLDER_OPT = "delete-older-than";
    public static final String DELETE_OLDER_DESC = "when --delete is used, add this option to only delete files that have been modified more than <delete-age> milliseconds ago";
    public static final String DELETE_OLDER_ARG_NAME = "delete-age";

    public static final String DELETE_CHECK_OPT = "delete-check-script";
    public static final String DELETE_CHECK_DESC = "when --delete is used, add this option to execute an external script to check whether a file should be deleted.  If the process exits with return code zero, the file is safe to delete.";
    public static final String DELETE_CHECK_ARG_NAME = "path-to-check-script";

    protected File rootFile;
    protected boolean useAbsolutePath = false;
    private long deleteOlderThan = 0;
    private File deleteCheckScript;

    private MimetypesFileTypeMap mimeMap;

    public FilesystemSource() {
        mimeMap = new MimetypesFileTypeMap();
    }

    /**
     * Override to provide a different InputStream implementation (i.e. TFileInputStream)
     */
    protected InputStream createInputStream(File f) throws IOException {
        return new FileInputStream(f);
    }

    /**
     * Override to provide a different File implementation (i.e. TFile)
     */
    protected File createFile(String path) {
        return new File(path);
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.startsWith(FILE_PREFIX);
    }

    @Override
    public Iterator<FileSyncObject> iterator() {
        return Arrays.asList(new FileSyncObject(rootFile, getRelativePath(rootFile))).iterator();
    }

    @Override
    public Iterator<FileSyncObject> childIterator(FileSyncObject syncObject) {
        if (syncObject.isDirectory())
            return new DirectoryIterator(syncObject.getRawSourceIdentifier(), syncObject.getRelativePath());
        else
            return null;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(ABSOLUTE_PATH_OPT).withDescription(ABSOLUTE_PATH_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(DELETE_OLDER_OPT).withDescription(DELETE_OLDER_DESC)
                .hasArg().withArgName(DELETE_OLDER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DELETE_CHECK_OPT).withDescription(DELETE_CHECK_DESC)
                .hasArg().withArgName(DELETE_CHECK_ARG_NAME).create());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        if (!sourceUri.startsWith(FILE_PREFIX))
            throw new ConfigurationException("source must start with " + FILE_PREFIX);

        try {
            rootFile = new File(new URI(sourceUri));
        } catch (URISyntaxException e) {
            throw new ConfigurationException("Invalid URI", e);
        }

        useAbsolutePath = line.hasOption(ABSOLUTE_PATH_OPT);

        if (line.hasOption(DELETE_OLDER_OPT)) {
            deleteOlderThan = Long.parseLong(line.getOptionValue(DELETE_OLDER_OPT));
        }
        if (line.hasOption(DELETE_CHECK_OPT)) {
            deleteCheckScript = new File(line.getOptionValue(DELETE_CHECK_OPT));
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (!rootFile.exists())
            throw new ConfigurationException("The source " + rootFile + " does not exist");

        if (deleteCheckScript != null && !deleteCheckScript.exists())
            throw new ConfigurationException("Delete check script " + deleteCheckScript + " does not exist");
    }

    @Override
    public String getName() {
        return "Filesystem Source";
    }

    @Override
    public String getDocumentation() {
        return "The filesystem source reads data from a file or directory.  " +
                "It is triggered by setting the source to a valid File URL:\n" +
                "file://<path>, e.g. file:///home/user/myfiles\n" +
                "If the URL refers to a file, only that file will be " +
                "transferred.  If a directory is specified, the contents of " +
                "the directory will be transferred.  Unless the --non-recursive" +
                "flag is set, the subdirectories will also be recursively " +
                "transferred.  By default, any metadata side-car files " +
                "will be assigned to their " +
                "target objects; use --" + CommonOptions.IGNORE_METADATA_OPTION +
                " to ignore the metadata directory.";
    }

    @Override
    public void delete(FileSyncObject syncObject) {
        delete(getMetaFile(syncObject.getRawSourceIdentifier()));
        delete(syncObject.getRawSourceIdentifier());
    }

    protected void delete(File file) {
        // Try to lock the file first.  If this fails, the file is
        // probably open for write somewhere.
        // Note that on a mac, you can apparently delete files that
        // someone else has open for writing, and can lock files
        // too.
        // Must make sure to throw exceptions when necessary to flag actual failures as opposed to skipped files.
        if (file.isDirectory()) {
            File metaDir = getMetaFile(file).getParentFile();
            if (metaDir.exists()) metaDir.delete();
            // Just try and delete dir
            if (!file.delete()) {
                LogMF.warn(l4j, "Failed to delete directory {0}", file);
            }
        } else {
            boolean tryDelete = true;
            if (deleteOlderThan > 0) {
                if (System.currentTimeMillis() - file.lastModified() < deleteOlderThan) {
                    LogMF.info(l4j, "not deleting {0}; it is not at least {1} ms old", file, deleteOlderThan);
                    tryDelete = false;
                }
            }
            if (deleteCheckScript != null) {
                String[] args = new String[]{
                        deleteCheckScript.getAbsolutePath(),
                        file.getAbsolutePath()
                };
                try {
                    l4j.debug("delete check: " + Arrays.asList(args));
                    Process p = Runtime.getRuntime().exec(args);
                    while (true) {
                        try {
                            int exitCode = p.exitValue();

                            if (exitCode == 0) {
                                LogMF.debug(l4j, "delete check OK, exit code {0}", exitCode);
                            } else {
                                LogMF.info(l4j, "delete check failed, exit code {0}.  Not deleting file.", exitCode);
                                tryDelete = false;
                            }
                            break;
                        } catch (IllegalThreadStateException e) {
                            // Ignore.
                        }
                    }
                } catch (IOException e) {
                    LogMF.info(l4j, "error executing delete check script: {0}.  Not deleting file.", e.toString());
                    tryDelete = false;
                }
            }
            RandomAccessFile raf = null;
            if (tryDelete) {
                try {
                    raf = new RandomAccessFile(file, "rw");
                    FileChannel fc = raf.getChannel();
                    FileLock flock = fc.lock();
                    // If we got here, we should be good.
                    flock.release();
                    if (!file.delete()) {
                        throw new RuntimeException(MessageFormat.format("Failed to delete {0}", file));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(MessageFormat.format("File {0} not deleted, it appears to be open: {1}",
                            file, e.getMessage()));
                } finally {
                    if (raf != null) {
                        try {
                            raf.close();
                        } catch (IOException e) {
                            // Ignore.
                        }
                    }
                }
            }
        }
    }

    protected File getMetaFile(File objectFile) {
        return createFile(SyncMetadata.getMetaPath(objectFile.getPath(), objectFile.isDirectory()));
    }

    protected SyncMetadata readMetadata(File objectFile) throws IOException {
        try (InputStream is = new BufferedInputStream(createInputStream(getMetaFile(objectFile)))) {
            return SyncMetadata.fromJson(new Scanner(is).useDelimiter("\\A").next());
        }
    }

    protected String getRelativePath(File file) {
        String relativePath = file.getAbsolutePath();
        if (!useAbsolutePath && rootFile != null && relativePath.startsWith(rootFile.getAbsolutePath())) {
            relativePath = relativePath.substring(rootFile.getAbsolutePath().length());
        }
        if (File.separatorChar == '\\') {
            relativePath = relativePath.replace('\\', '/');
        }
        if (relativePath.startsWith("/")) {
            relativePath = relativePath.substring(1);
        }
        return relativePath;
    }

    public class FileSyncObject extends SyncObject<File> {
        public FileSyncObject(File sourceFile, String relativePath) {
            super(sourceFile, sourceFile.getAbsolutePath(), relativePath, sourceFile.isDirectory());
        }

        @Override
        protected void loadObject() {
            // first check for a "side-car" file for metadata from an object system
            if (!ignoreMetadata && getMetaFile(getRawSourceIdentifier()).exists()) {
                try {
                    metadata = readMetadata(getRawSourceIdentifier());
                } catch (IOException e) {
                    throw new RuntimeException("Could not read metadata file for " + getRawSourceIdentifier(), e);
                }
                // otherwise collect filesystem metadata
            } else {
                metadata = FilesystemUtil.createFilesystemMetadata(getRawSourceIdentifier(), mimeMap, includeAcl);
            }
        }

        @Override
        public synchronized InputStream createSourceInputStream() {
            try {
                return getRawSourceIdentifier().isDirectory() ? null :
                        new BufferedInputStream(createInputStream(getRawSourceIdentifier()), bufferSize);
            } catch (IOException e) {
                throw new RuntimeException("Could not open source file:" + getRawSourceIdentifier(), e);
            }
        }
    }

    public class DirectoryIterator extends ReadOnlyIterator<FileSyncObject> {
        private Iterator<File> childFiles;
        private String relativePath;

        public DirectoryIterator(File directory, String relativePath) {
            File[] files = directory.listFiles();
            if (files == null) files = new File[]{};
            childFiles = Arrays.asList(files).iterator();
            this.relativePath = relativePath;
        }

        @Override
        protected FileSyncObject getNextObject() {
            while (childFiles.hasNext()) {
                File child = childFiles.next();
                if (SyncMetadata.METADATA_DIR.equals(child.getName()) || SyncMetadata.DIR_META_FILE.equals(child.getName()))
                    continue;
                String childPath = relativePath + "/" + child.getName();
                childPath = childPath.replaceFirst("^/", "");
                return new FileSyncObject(child, childPath);
            }
            return null;
        }

    }

    public File getRootFile() {
        return rootFile;
    }

    public void setRootFile(File rootFile) {
        this.rootFile = rootFile;
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
}
