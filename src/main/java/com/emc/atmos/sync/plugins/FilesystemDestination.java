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

import com.emc.atmos.sync.util.AtmosMetadata;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * The FilesystemDestination writes files to a local filesystem.
 *
 * @author cwikj
 */
public class FilesystemDestination extends DestinationPlugin {
    private static final Logger l4j = Logger.getLogger(
            FilesystemDestination.class);

    public static final String NO_META_OPT = "no-fs-metadata";
    public static final String NO_META_DESC = "Disables writing metadata, ACL, and content type information to the " + AtmosMetadata.META_DIR + " directory";

    protected File destination;
    protected boolean noMetadata = false;
    protected boolean force = false;

    /**
     * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
     */
    @Override
    public void filter(SyncObject obj) {
        File destFile = getFile(destination.getPath(), obj.getRelativePath());
        obj.setDestURI(destFile.toURI());

        LogMF.debug(l4j, "Writing {0} to {1}", obj.getSourceURI(), destFile);

        Date mtime = obj.getMetadata().getMtime();

        // make sure parent directory exists (it should already)
        mkdirs(destFile.getParentFile());

        if (obj.isDirectory()) {
            if (!destFile.exists() && !destFile.mkdir())
                throw new RuntimeException("Failed to create directory " + destFile);
        } else {
            // If newer or forced, copy the file data
            if (force || mtime == null || !destFile.exists() || mtime.after(new Date(destFile.lastModified()))) {
                copyData(obj, destFile);
            } else {
                LogMF.debug(l4j, "No change in content timestamps for {0}", obj.getSourceURI());
            }
        }

        // set mtime (for directories, this may be overwritten if any children are modified)
        if (mtime != null)
            destFile.setLastModified(mtime.getTime());

        if (!noMetadata) {
            File metaFile = getFile(null, AtmosMetadata.getMetaPath(destFile.getPath(), obj.isDirectory()));
            File metaDir = metaFile.getParentFile();

            Date ctime = obj.getMetadata().getCtime();
            if (ctime == null) ctime = mtime; // use mtime if there is no ctime

            // create metadata directory if it doesn't already exist
            if (!metaDir.exists() && !metaDir.mkdir())
                throw new RuntimeException("Failed to create metadata directory " + metaDir);

            // if *ctime* is newer or forced, write the metadata file
            if (force || ctime == null || !metaFile.exists() || ctime.after(new Date(metaFile.lastModified()))) {
                try {
                    obj.getMetadata().toStream(getFileStream(metaFile));
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
                LogMF.debug(l4j, "No change in metadata for {0}", obj.getSourceURI());
            }
        }
    }

    /**
     * Override to provide a custom File implementation
     */
    protected File getFile(String rootPath, String relativePath) {
        return new File(rootPath, relativePath);
    }

    /**
     * Override to provide a custom file stream implementation
     */
    protected OutputStream getFileStream(File file) throws IOException {
        return new FileOutputStream(file);
    }

    private void copyData(SyncObject obj, File destFile) {
        byte[] buffer = new byte[65536];

        int c = 0;
        InputStream in = null;
        OutputStream out = null;
        try {
            in = obj.getInputStream();
            out = getFileStream(destFile);
            while ((c = in.read(buffer)) != -1) {
                out.write(buffer, 0, c);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing: " + destFile +
                    ": " + e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // Ignore
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    // Ignore
                }
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

    /**
     * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
     */
    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        Options opts = new Options();

        opts.addOption(OptionBuilder.withDescription(NO_META_DESC)
                .withLongOpt(NO_META_OPT).create());

        return opts;
    }

    /**
     * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
     */
    @Override
    public boolean parseOptions(CommandLine line) {
        String destOption = line.getOptionValue(CommonOptions.DESTINATION_OPTION);
        if (destOption != null && destOption.startsWith("file://")) {
            URI u;
            try {
                u = new URI(destOption);
            } catch (URISyntaxException e) {
                throw new RuntimeException("Failed to parse URI: " + destOption + ": " + e.getMessage(), e);
            }
            destination = new File(u);

            if (line.hasOption(NO_META_OPT)) {
                noMetadata = true;
            }

            if (line.hasOption(CommonOptions.FORCE_OPTION)) {
                force = true;
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
        return "Filesystem Destination";
    }

    /**
     * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
     */
    @Override
    public String getDocumentation() {
        return "The filesystem desination writes data to a file or directory.  " +
                "It is triggered by setting the desination to a valid File URL:\n" +
                "file://<path>, e.g. file:///home/user/myfiles\n" +
                "If the URL refers to a file, only that file will be " +
                "transferred.  If a directory is specified, the source " +
                "contents will be written into the directory.  By default, " +
                "Atmos metadata, ACLs, and content type will be written to " +
                "a file with the same name inside the " +
                AtmosMetadata.META_DIR + " directory.  Use the " +
                NO_META_OPT + " to skip writing the metadata directory.  By " +
                "default, this plugin will check the mtime on the file and " +
                "its metadata file and only update if the source mtime and " +
                "ctime are later, respectively.  Use the --" +
                CommonOptions.FORCE_OPTION + " to override this behavior and " +
                "always overwrite files.";
    }
}
