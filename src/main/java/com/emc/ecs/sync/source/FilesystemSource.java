/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.source;

import com.emc.ecs.sync.CommonOptions;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.SyncEstimate;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.FileSyncObject;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.regex.Pattern;

/**
 * The filesystem source reads data from a file or directory.
 */
public class FilesystemSource extends SyncSource<FileSyncObject> {
    private static final Logger log = LoggerFactory.getLogger(FilesystemSource.class);

    protected static final String FILE_PREFIX = "file://";

    public static final String ABSOLUTE_PATH_OPT = "use-absolute-path";
    public static final String ABSOLUTE_PATH_DESC = "Uses the absolute path to the file when storing it instead of the relative path from the source dir.";

    public static final String DELETE_OLDER_OPT = "delete-older-than";
    public static final String DELETE_OLDER_DESC = "when --delete is used, add this option to only delete files that have been modified more than <delete-age> milliseconds ago";
    public static final String DELETE_OLDER_ARG_NAME = "delete-age";

    public static final String DELETE_CHECK_OPT = "delete-check-script";
    public static final String DELETE_CHECK_DESC = "when --delete is used, add this option to execute an external script to check whether a file should be deleted.  If the process exits with return code zero, the file is safe to delete.";
    public static final String DELETE_CHECK_ARG_NAME = "path-to-check-script";

    public static final String FOLLOW_LINKS_OPT = "follow-links";
    public static final String FOLLOW_LINKS_DESC = "instead of preserving symbolic links, follow them and sync the actual files";

    public static final String EXCLUDE_FILENAMES_OPT = "exclude-filenames";
    public static final String EXCLUDE_FILENAMES_DESC = "(deprecated - use exclude-paths) A list of regular expressions to search against the file name.  If the name matches, the file will be skipped.  Since this is a regular expression, take care to escape special characters.  For example, to exclude all filenames that begin with a period, the pattern would be \\..*";
    public static final String EXCLUDE_FILENAMES_ARG_NAME = "pattern,pattern,...";

    public static final String EXCLUDE_PATHS_OPT = "exclude-paths";
    public static final String EXCLUDE_PATHS_DESC = "A list of regular expressions to search against the full file path.  If the path matches, the file will be skipped.  Since this is a regular expression, take care to escape special characters.  For example, to exclude all files and directories that begin with a period, the pattern would be .*/\\..*";
    public static final String EXCLUDE_PATHS_ARG_NAME = "pattern,pattern,...";

    public static final String LIST_OPT = "file-list";
    public static final String LIST_DESC = "Specifies a list-file that contains the files or directories to include in the sync (one path per line). Only these files will be included. Paths must be relative to the source dir.";
    public static final String LIST_ARG_NAME = "list-file";

    protected File rootFile;
    protected boolean useAbsolutePath = false;
    private long deleteOlderThan = 0;
    private File deleteCheckScript;
    private Date modifiedSince;
    private boolean followLinks = false;
    private FilenameFilter filter;
    private List<String> excludedFilenames;
    private List<Pattern> excludedFilenamePatterns;
    private List<String> excludedPaths;
    private List<Pattern> excludedPathPatterns;
    private File fileList;

    protected MimetypesFileTypeMap mimeMap;

    public FilesystemSource() {
        mimeMap = new MimetypesFileTypeMap();
        filter = new SourceFilter();
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.startsWith(FILE_PREFIX);
    }

    @Override
    public SyncEstimate createEstimate() {
        SyncEstimate estimate = new SyncEstimate();
        final EnhancedThreadPoolExecutor dirExecutor = new EnhancedThreadPoolExecutor(8, new LinkedBlockingDeque<Runnable>(), "dirEstimator");
        final EnhancedThreadPoolExecutor fileExecutor = new EnhancedThreadPoolExecutor(8, new LinkedBlockingDeque<Runnable>(100), "fileEstimator");

        if (fileList != null) {
            final Iterator<String> fileIterator = new FileLineIterator(fileList);
            while (fileIterator.hasNext()) {
                dirExecutor.submit(new EstimateTask(new File(fileIterator.next().trim()), estimate, dirExecutor, fileExecutor));
            }
        } else {
            dirExecutor.submit(new EstimateTask(rootFile, estimate, dirExecutor, fileExecutor));
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        if (dirExecutor.getActiveCount() + fileExecutor.getActiveCount() == 0) {
                            dirExecutor.shutdown();
                            fileExecutor.shutdown();
                            break;
                        }
                        Thread.sleep(1000);
                    } catch (Throwable t) {
                        log.warn("unexpected exception in estimation monitor", t);
                    }
                }
            }
        }).start();

        return estimate;
    }

    @Override
    public boolean veto(FileSyncObject syncObject) {
        return veto(syncObject.getRawSourceIdentifier()) || super.veto(syncObject);
    }

    protected boolean veto(File file) {
        if (SyncMetadata.METADATA_DIR.equals(file.getName()) || SyncMetadata.DIR_META_FILE.equals(file.getName()))
            return true;
        if (modifiedSince != null && file.lastModified() < modifiedSince.getTime())
            return true;
        return false;
    }

    @Override
    public Iterator<FileSyncObject> iterator() {
        if (fileList != null) {
            return fileListIterator();
        } else {
            return Collections.singletonList(new FileSyncObject(this, mimeMap, rootFile, getRelativePath(rootFile),
                    followLinks)).iterator();
        }
    }

    @Override
    public Iterator<FileSyncObject> childIterator(FileSyncObject syncObject) {
        if (syncObject.isDirectory())
            return new DirectoryIterator(syncObject.getRawSourceIdentifier(), syncObject.getRelativePath());
        else
            return null;
    }

    public Iterator<FileSyncObject> fileListIterator() {
        final Iterator<String> fileIterator = new FileLineIterator(fileList);

        return new ReadOnlyIterator<FileSyncObject>() {
            @Override
            protected FileSyncObject getNextObject() {
                if (fileIterator.hasNext()) {
                    File file = new File(rootFile, fileIterator.next().trim());
                    return new FileSyncObject(FilesystemSource.this, mimeMap, file, getRelativePath(file), followLinks);
                }
                return null;
            }
        };
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(ABSOLUTE_PATH_OPT).desc(ABSOLUTE_PATH_DESC).build());
        opts.addOption(Option.builder().longOpt(DELETE_OLDER_OPT).desc(DELETE_OLDER_DESC)
                .hasArg().argName(DELETE_OLDER_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(DELETE_CHECK_OPT).desc(DELETE_CHECK_DESC)
                .hasArg().argName(DELETE_CHECK_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(FOLLOW_LINKS_OPT).desc(FOLLOW_LINKS_DESC).build());
        opts.addOption(Option.builder().longOpt(EXCLUDE_FILENAMES_OPT).desc(EXCLUDE_FILENAMES_DESC)
                .hasArgs().argName(EXCLUDE_FILENAMES_ARG_NAME).valueSeparator(',').build());
        opts.addOption(Option.builder().longOpt(EXCLUDE_PATHS_OPT).desc(EXCLUDE_PATHS_DESC)
                .hasArgs().argName(EXCLUDE_PATHS_ARG_NAME).valueSeparator(',').build());
        opts.addOption(Option.builder().longOpt(LIST_OPT).desc(LIST_DESC)
                .hasArgs().argName(LIST_ARG_NAME).valueSeparator(',').build());
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

        followLinks = line.hasOption(FOLLOW_LINKS_OPT);

        if (line.hasOption(EXCLUDE_FILENAMES_OPT)) {
            log.warn(EXCLUDE_FILENAMES_OPT + " is deprected; please use " + EXCLUDE_PATHS_OPT);
            excludedFilenames = Arrays.asList(line.getOptionValues(EXCLUDE_FILENAMES_OPT));
        }

        if (line.hasOption(EXCLUDE_PATHS_OPT)) {
            excludedPaths = Arrays.asList(line.getOptionValues(EXCLUDE_PATHS_OPT));
        }

        if (line.hasOption(LIST_OPT)) {
            fileList = new File(line.getOptionValue(LIST_OPT));
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (!rootFile.exists())
            throw new ConfigurationException("The source " + rootFile + " does not exist");

        if (deleteCheckScript != null && !deleteCheckScript.exists())
            throw new ConfigurationException("Delete check script " + deleteCheckScript + " does not exist");

        if (excludedFilenames != null) {
            excludedFilenamePatterns = new ArrayList<>();
            for (String pattern : excludedFilenames) {
                excludedFilenamePatterns.add(Pattern.compile(pattern));
            }
        }

        if (excludedPaths != null) {
            excludedPathPatterns = new ArrayList<>();
            for (String pattern : excludedPaths) {
                excludedPathPatterns.add(Pattern.compile(pattern));
            }
        }

        if (fileList != null && !fileList.exists())
            throw new ConfigurationException("File list " + fileList.getPath() + " does not exist");
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
        syncObject.delete(deleteOlderThan, deleteCheckScript);
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

    public class DirectoryIterator extends ReadOnlyIterator<FileSyncObject> {
        private Iterator<File> childFiles;
        private String relativePath;

        public DirectoryIterator(File directory, String relativePath) {
            File[] files = directory.listFiles(filter);
            if (files == null) files = new File[]{};
            childFiles = Arrays.asList(files).iterator();
            this.relativePath = relativePath;
        }

        @Override
        protected FileSyncObject getNextObject() {
            while (childFiles.hasNext()) {
                File child = childFiles.next();
                if (SyncMetadata.METADATA_DIR.equals(child.getName())) continue; // don't recurse into the metadata dir
                String childPath = relativePath + "/" + child.getName();
                childPath = childPath.replaceFirst("^/", "");
                return new FileSyncObject(FilesystemSource.this, mimeMap, child, childPath, followLinks);
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

    public Date getModifiedSince() {
        return modifiedSince;
    }

    public void setModifiedSince(Date modifiedSince) {
        this.modifiedSince = modifiedSince;
    }

    public boolean isFollowLinks() {
        return followLinks;
    }

    public void setFollowLinks(boolean followLinks) {
        this.followLinks = followLinks;
    }

    public List<String> getExcludedFilenames() {
        return excludedFilenames;
    }

    /**
     * @deprecated (2.0.1) use {@link #setExcludedPaths(List)} instead
     */
    public void setExcludedFilenames(List<String> excludedFilenames) {
        this.excludedFilenames = excludedFilenames;
    }

    public List<String> getExcludedPaths() {
        return excludedPaths;
    }

    public void setExcludedPaths(List<String> excludedPaths) {
        this.excludedPaths = excludedPaths;
    }

    public File getFileList() {
        return fileList;
    }

    public void setFileList(File fileList) {
        this.fileList = fileList;
    }

    public FilenameFilter getFilter() {
        return filter;
    }

    class SourceFilter implements FilenameFilter {
        @Override
        public boolean accept(File dir, String name) {
            File target = new File(dir, name);
            if (excludedPathPatterns != null) {
                for (Pattern p : excludedPathPatterns) {
                    if (p.matcher(target.getPath()).matches() || p.matcher(name).matches()) {
                        if (log.isDebugEnabled()) log.debug("Skipping file {}: matches pattern: {}", target, p);
                        return false;
                    }
                }
            }
            if (excludedFilenamePatterns != null) {
                for (Pattern p : excludedFilenamePatterns) {
                    if (p.matcher(name).matches()) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping file {}: matches filename pattern: {}", target, p);
                        return false;
                    }
                }
            }

            return true;
        }
    }

    class EstimateTask implements Runnable {
        private File file;
        private SyncEstimate estimate;
        private EnhancedThreadPoolExecutor dirExecutor;
        private EnhancedThreadPoolExecutor fileExecutor;

        public EstimateTask(File file, SyncEstimate estimate, EnhancedThreadPoolExecutor dirExecutor,
                            EnhancedThreadPoolExecutor fileExecutor) {
            this.file = file;
            this.estimate = estimate;
            this.dirExecutor = dirExecutor;
            this.fileExecutor = fileExecutor;
        }

        @Override
        public void run() {
            try {
                if (veto(file)) return;
                estimate.incTotalObjectCount(1);
                if (followLinks || !FilesystemUtil.isSymLink(file)) { // don't recurse or tally symlinks unless we should
                    if (file.isDirectory()) {
                        File[] children = file.listFiles(filter);
                        if (children != null) {
                            for (File child : children) {
                                if (child.isDirectory())
                                    dirExecutor.blockingSubmit(new EstimateTask(child, estimate, dirExecutor, fileExecutor));
                                else
                                    fileExecutor.blockingSubmit(new EstimateTask(child, estimate, dirExecutor, fileExecutor));
                            }
                        }
                    } else {
                        estimate.incTotalByteCount(file.length());
                    }
                }
            } catch (Throwable t) {
                log.warn("unexpected exception", t);
            }
        }
    }
}
