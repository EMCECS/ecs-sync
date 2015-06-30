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
import com.emc.vipr.sync.model.object.FileSyncObject;
import com.emc.vipr.sync.model.object.S3SyncObject;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.ReadOnlyIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * The filesystem source reads data from a file or directory.
 */
public class FilesystemSource extends SyncSource<FileSyncObject> {
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

    protected MimetypesFileTypeMap mimeMap;

    public FilesystemSource() {
        mimeMap = new MimetypesFileTypeMap();
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.startsWith(FILE_PREFIX);
    }

    @Override
    public Iterator<FileSyncObject> iterator() {
        return Arrays.asList(new FileSyncObject(this, mimeMap, rootFile, getRelativePath(rootFile))).iterator();
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
        syncObject.delete(deleteOlderThan, deleteCheckScript);
    }

//    @Override
//    public void delete(S3SyncObject syncObject) {
//
//    }

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
                return new FileSyncObject(FilesystemSource.this, mimeMap, child, childPath);
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
