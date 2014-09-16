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
package com.emc.vipr.sync.target;

import com.emc.vipr.sync.CommonOptions;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.FilesystemUtil;
import com.emc.vipr.sync.util.Iso8601Util;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Iterator;

/**
 * The FilesystemTarget writes files to a local filesystem.
 *
 * @author cwikj
 */
public class FilesystemTarget extends SyncTarget {
    private static final Logger l4j = Logger.getLogger(FilesystemTarget.class);

    protected static final String FILE_PREFIX = "file://";

    protected File targetRoot;

    /**
     * Override to provide a different OutputStream implementation (i.e. TFileOutputStream)
     */
    protected OutputStream createOutputStream(File f) throws IOException {
        return new FileOutputStream(f);
    }

    /**
     * Override to provide a different File implementation (i.e. TFile)
     */
    protected File createFile(String parentPath, String subPath) {
        return new File(parentPath, subPath);
    }

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.startsWith(FILE_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        return new Options();
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        if (!targetUri.startsWith(FILE_PREFIX))
            throw new ConfigurationException("target must start with " + FILE_PREFIX);

        try {
            targetRoot = new File(new URI(targetUri));
        } catch (URISyntaxException e) {
            throw new ConfigurationException("Invalid URI", e);
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.notNull(targetRoot, "you must specify a target file/directory");
    }

    @Override
    public void filter(SyncObject obj) {
        File destFile = createFile(targetRoot.getPath(), obj.getRelativePath());
        obj.setTargetIdentifier(destFile.getPath());

        LogMF.debug(l4j, "Writing {0} to {1}", obj.getSourceIdentifier(), destFile);

        Date mtime = obj.getMetadata().getModifiedTime();

        // make sure parent directory exists
        mkdirs(destFile.getParentFile());

        if (obj.hasChildren()) {
            synchronized (this) {
                if (!destFile.exists() && !destFile.mkdir())
                    throw new RuntimeException("Failed to create directory " + destFile);
            }
        }

        if (obj.hasData()) {
            File dataFile = destFile;
            // if the object has both children and data, we need to create a data file for the target directory
            if (obj.hasChildren()) dataFile = new File(FilesystemUtil.getDirDataPath(destFile.getPath()));
            // If newer or forced, copy the file data
            if (force || mtime == null || !dataFile.exists() || mtime.after(new Date(dataFile.lastModified()))) {
                copyData(obj, dataFile);
            } else {
                LogMF.debug(l4j, "No change in content timestamps for {0}", obj.getSourceIdentifier());
            }
        }

        // set mtime (for directories, note this may be overwritten if any children are modified)
        if (mtime != null)
            destFile.setLastModified(mtime.getTime());

        if (!ignoreMetadata) {
            File metaFile = createFile(null, SyncMetadata.getMetaPath(destFile.getPath(), destFile.isDirectory()));
            File metaDir = metaFile.getParentFile();

            Date ctime = Iso8601Util.parse(obj.getMetadata().getSystemMetadataProp("ctime"));
            if (ctime == null) ctime = mtime; // use mtime if there is no ctime

            // create metadata directory if it doesn't already exist
            synchronized (this) {
                if (!metaDir.exists() && !metaDir.mkdir())
                    throw new RuntimeException("Failed to create metadata directory " + metaDir);
            }

            // if *ctime* is newer or forced, write the metadata file
            if (force || ctime == null || !metaFile.exists() || ctime.after(new Date(metaFile.lastModified()))) {
                try {
                    String metaJson = obj.getMetadata().toJson();
                    copyData(new ByteArrayInputStream(metaJson.getBytes("UTF-8")), createOutputStream(metaFile));
                    if (ctime != null) {
                        // Set the metadata file mtime to the source ctime (i.e. this
                        // metadata file's content is modified at the same
                        // time as the source's metadata modification time)
                        metaFile.setLastModified(ctime.getTime());
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write metadata to: " + metaFile, e);
                }
            } else {
                LogMF.debug(l4j, "No change in metadata for {0}", obj.getSourceIdentifier());
            }
        }
    }

    private void copyData(SyncObject obj, File destFile) {
        try {
            copyData(obj.getInputStream(), createOutputStream(destFile));
        } catch (IOException e) {
            throw new RuntimeException("Error writing: " + destFile + ": " + e.getMessage(), e);
        }
    }

    private void copyData(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[65536];
        int c;
        try (InputStream in = inputStream; OutputStream out = outputStream) {
            while ((c = in.read(buffer)) != -1) {
                out.write(buffer, 0, c);
            }
        } // inputStream and outputStream will be closed automatically
    }

    /**
     * Synchronized mkdir to prevent conflicts in threaded environment.
     */
    private static synchronized void mkdirs(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create directory " +
                        dir);
            }
        }
    }

    @Override
    public String getName() {
        return "Filesystem Target";
    }

    @Override
    public String getDocumentation() {
        return "The filesystem desination writes data to a file or directory.  " +
                "It is triggered by setting the desination to a valid File URL:\n" +
                "file://<path>, e.g. file:///home/user/myfiles\n" +
                "If the URL refers to a file, only that file will be " +
                "transferred.  If a directory is specified, the source " +
                "contents will be written into the directory.  By default, " +
                "metadata will be written to " +
                "a file with the same name inside the " +
                SyncMetadata.METADATA_DIR + " directory.  Use the " +
                CommonOptions.IGNORE_METADATA_OPTION + " to skip writing the metadata directory.  By " +
                "default, this plugin will check the mtime on the file and " +
                "its metadata file and only update if the source mtime and " +
                "ctime are later, respectively.  Use the --" +
                CommonOptions.FORCE_OPTION + " to override this behavior and " +
                "always overwrite files.";
    }

    public File getTargetRoot() {
        return targetRoot;
    }

    public void setTargetRoot(File targetRoot) {
        this.targetRoot = targetRoot;
    }
}
