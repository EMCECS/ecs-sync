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
package com.emc.ecs.sync.target;

import com.emc.ecs.sync.CommonOptions;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.AtmosMetadata;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.FileSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.FilesystemSource;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.util.ConfigurationException;
import com.emc.ecs.sync.util.FilesystemUtil;
import com.emc.ecs.sync.util.Iso8601Util;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Iterator;

import static com.emc.ecs.sync.model.SyncMetadata.UserMetadata;

/**
 * The FilesystemTarget writes files to a local filesystem.
 *
 * @author cwikj
 */
public class FilesystemTarget extends SyncTarget {
    private static final Logger log = LoggerFactory.getLogger(FilesystemTarget.class);

    protected static final String FILE_PREFIX = "file://";

    protected File targetRoot;
    protected boolean followLinks = false;

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

        // in case, we're pulling from another filesystem, make sure link behavior is consistent
        if (source instanceof FilesystemSource) followLinks = ((FilesystemSource) source).isFollowLinks();
    }

    @Override
    public void filter(SyncObject obj) {
        File destFile = createFile(targetRoot.getPath(), obj.getRelativePath());
        obj.setTargetIdentifier(destFile.getPath());

        log.debug("Writing {} to {}", obj.getSourceIdentifier(), destFile);

        Date mtime = FilesystemUtil.getMtime(obj);

        // make sure parent directory exists
        mkdirs(destFile.getParentFile());

        // if required we will need to update metadata after any streaming operation
        boolean dataCopied = false;

        if (obj.isDirectory()) {
            synchronized (this) {
                if (!destFile.exists() && !destFile.mkdir())
                    throw new RuntimeException("Failed to create directory " + destFile);
            }
        } else {
            // If forced, retrying, newer or different size, copy the file data
            if (force || obj.getFailureCount() > 0 || mtime == null || !destFile.exists()
                    || mtime.after(new Date(destFile.lastModified())) || obj.getMetadata().getContentLength() != destFile.length()) {
                copyData(obj, destFile);
                dataCopied = true;
            } else {
                log.debug("No change in content timestamps for {}", obj.getSourceIdentifier());
            }
        }

        // encapsulate metadata from source system
        if (!ignoreMetadata) {
            File metaFile = createFile(null, SyncMetadata.getMetaPath(destFile.getPath(), destFile.isDirectory()));
            File metaDir = metaFile.getParentFile();

            Date ctime = null;
            if (obj.getMetadata() instanceof AtmosMetadata) {
                // check for ctime in system meta
                UserMetadata m = ((AtmosMetadata) obj.getMetadata()).getSystemMetadata().get("ctime");
                if (m != null) ctime = Iso8601Util.parse(m.getValue());
            }
            if (ctime == null) ctime = mtime; // use mtime if there is no ctime

            // create metadata directory if it doesn't already exist
            synchronized (this) {
                if (!metaDir.exists() && !metaDir.mkdir())
                    throw new RuntimeException("Failed to create metadata directory " + metaDir);
            }

            // if *ctime* is newer or forced, write the metadata file.. also if object has generated new metadata from
            // a streaming operation
            if (force || ctime == null || !metaFile.exists() || ctime.after(new Date(metaFile.lastModified()))
                    || (dataCopied && obj.requiresPostStreamMetadataUpdate())) {
                try {
                    String metaJson = obj.getMetadata().toJson();
                    copyData(new ByteArrayInputStream(metaJson.getBytes("UTF-8")), metaFile);
                    if (ctime != null) {
                        // Set the metadata file mtime to the source ctime (i.e. this
                        // metadata file's content is modified at the same
                        // time as the source's metadata modification time)
                        if (!metaFile.setLastModified(ctime.getTime()))
                            log.warn("Failed to set mtime of {}", metaFile);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write metadata to: " + metaFile, e);
                }
            } else {
                log.debug("No change in metadata for {}", obj.getSourceIdentifier());
            }
        }

        try {
            // TODO: figure out "preserve"/"restore" option
            // TODO: make the behavior here configurable (do we fail? do we track in the DB?)
            FilesystemUtil.applyFilesystemMetadata(destFile, obj.getMetadata(), includeAcl, true);
        } catch (Exception e) {
            log.warn("could not apply filesystem metadata to " + destFile, e);
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        File destFile = createFile(targetRoot.getPath(), obj.getRelativePath());
        obj.setTargetIdentifier(destFile.getPath());
        return new FileSyncObject(this, new MimetypesFileTypeMap(), createFile(targetRoot.getPath(), obj.getRelativePath()),
                obj.getRelativePath(), followLinks);
    }

    private void copyData(SyncObject obj, File destFile) {
        try {
            if (FilesystemUtil.isSymLink(obj)) { // restore a sym link
                String targetPath = FilesystemUtil.getLinkTarget(obj);
                if (targetPath == null)
                    throw new RuntimeException("object appears to be a symbolic link, but no target path was found");

                log.info("re-creating symbolic link {} -> {}", obj.getRelativePath(), targetPath);

                if (destFile.exists() && FilesystemUtil.isSymLink(destFile) && !destFile.delete())
                    throw new RuntimeException("could not overwrite existing link");

                Files.createSymbolicLink(destFile.toPath(), Paths.get(targetPath));
            } else {
                copyData(obj.getInputStream(), destFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing: " + destFile + ": " + e.getMessage(), e);
        }
    }

    private void copyData(InputStream inStream, File outFile) throws IOException {
        byte[] buffer = new byte[128 * 1024];
        int c;
        try (InputStream input = inStream; OutputStream output = createOutputStream(outFile)) {
            while ((c = input.read(buffer)) != -1) {
                output.write(buffer, 0, c);
                if (monitorPerformance) getWritePerformanceCounter().increment(c);
            }
        }
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
        return "The filesystem target writes data to a file or directory.  " +
                "It is triggered by setting the target to a valid File URL:\n" +
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
