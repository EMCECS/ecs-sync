/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
package com.emc.atmos.sync.plugins.cas;

import com.emc.atmos.sync.plugins.DestinationPlugin;
import com.emc.atmos.sync.plugins.SyncObject;
import com.emc.atmos.sync.plugins.SyncPlugin;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPTag;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Date;

public class CuaFilesystemDestination extends DestinationPlugin implements InitializingBean {
    private static final Logger l4j = Logger.getLogger(CuaFilesystemDestination.class);

    private static final String CLIP_NAME = "Storigen_File_Gateway";
    private static final String ATTRIBUTE_TAG_NAME = "Storigen_File_Gateway_File_Attributes";
    private static final String BLOB_TAG_NAME = "Storigen_File_Gateway_Blob";

    private static final String PATH_ATTRIBUTE = "Storigen_File_Gateway_File_Path0";
    private static final String UID_ATTRIBUTE = "Storigen_File_Gateway_File_Owner";
    private static final String GID_ATTRIBUTE = "Storigen_File_Gateway_File_Group";
    private static final String ITIME_ATTRIBUTE = "Storigen_File_Gateway_File_CTime"; // Windows ctime means create time
    private static final String MTIME_ATTRIBUTE = "Storigen_File_Gateway_File_MTime";
    private static final String ATIME_ATTRIBUTE = "Storigen_File_Gateway_File_ATime";
    private static final String SIZE_HI_ATTRIBUTE = "Storigen_File_Gateway_File_Size_Hi";
    private static final String SIZE_LO_ATTRIBUTE = "Storigen_File_Gateway_File_Size_Lo";

    private String destination;
    private File destinationDir;
    private boolean force = false;

    @Override
    public void filter(SyncObject obj) {
        timeOperationStart(CasUtil.OPERATION_TOTAL);

        if (!(obj instanceof ClipSyncObject))
            throw new UnsupportedOperationException("sync object was not a CAS clip");
        final ClipSyncObject clipSync = (ClipSyncObject) obj;

        try {
            FPClip sourceClip = clipSync.getClip();

            // looking for clips with a specific name
            if (!sourceClip.getName().equals(CLIP_NAME))
                throw new RuntimeException(String.format("skipped clip %s (clip name did not match)", clipSync.getClipId()));

            ClipTag blobTag = null;
            String filePath = null;
            Date itime = null, mtime = null, atime = null;
            long uid = 0, gid = 0, hi = 0, lo = 0;
            for (ClipTag clipTag : clipSync.getTags()) {
                FPTag sourceTag = clipTag.getTag();
                if (sourceTag.getTagName().equals(ATTRIBUTE_TAG_NAME)) {
                    // pull all pertinent attributes
                    filePath = sourceTag.getStringAttribute(PATH_ATTRIBUTE);
                    uid = sourceTag.getLongAttribute(UID_ATTRIBUTE);
                    gid = sourceTag.getLongAttribute(GID_ATTRIBUTE);
                    itime = new Date(sourceTag.getLongAttribute(ITIME_ATTRIBUTE));
                    mtime = new Date(sourceTag.getLongAttribute(MTIME_ATTRIBUTE));
                    atime = new Date(sourceTag.getLongAttribute(ATIME_ATTRIBUTE));
                    hi = sourceTag.getLongAttribute(SIZE_HI_ATTRIBUTE);
                    lo = sourceTag.getLongAttribute(SIZE_LO_ATTRIBUTE);
                } else if (sourceTag.getTagName().equals(BLOB_TAG_NAME)) {
                    blobTag = clipTag;
                }
            }

            // sanity check
            if (blobTag == null)
                throw new RuntimeException("could not find blob tag");
            if (filePath == null)
                throw new RuntimeException("could not find file path attribute");
            // assume the rest have been pulled

            // make file path relative
            if (filePath.startsWith("/")) filePath = filePath.substring(1);

            File destFile = new File(destinationDir, filePath);
            obj.setDestURI(destFile.toURI());

            LogMF.debug(l4j, "Writing {0} to {1}", obj.getSourceURI(), destFile);

            // make parent directories
            mkdirs(destFile.getParentFile());

            // Check timestamp if needed.
            long size = (lo > 0 ? hi << 32 : hi) + lo;
            if (!force && destFile.exists()) {
                Date destMtime = new Date(destFile.lastModified());
                if (!mtime.after(destMtime) && size == destFile.length()) {
                    LogMF.debug(l4j, "No change in blob timestamps for {0}", destFile);
                }
            } else {
                // write the file
                OutputStream out = new FileOutputStream(destFile);
                try {
                    blobTag.writeToStream(out);
                } finally {
                    try {
                        out.close();
                    } catch (Throwable t) {
                        l4j.warn("could not close destination file", t);
                    }
                }
            }

            Path destPath = destFile.toPath();

            // set times
            Files.setAttribute(destPath, "creationTime", FileTime.fromMillis(itime.getTime()));
            Files.setAttribute(destPath, "lastModifiedTime", FileTime.fromMillis(mtime.getTime()));
            Files.setAttribute(destPath, "lastAccessTime", FileTime.fromMillis(atime.getTime()));

            // set ownership if on unix-like OS
            if (!System.getProperty("os.name").startsWith("Windows")) {
                Files.setAttribute(destPath, "unix:uid", (int) uid);
                Files.setAttribute(destPath, "unix:gid", (int) gid);
            }

            // verify size
            if (size != destFile.length())
                throw new RuntimeException(String.format("destination file %s is not the right size (expected %d)", destFile, size));

            timeOperationComplete(CasUtil.OPERATION_TOTAL);
        } catch (Throwable t) {
            timeOperationFailed(CasUtil.OPERATION_TOTAL);
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            throw new RuntimeException("Failed to sync object: " + t.getMessage(), t);
        }
    }

    /**
     * Synchronized mkdir to prevent conflicts in threaded environment.
     */
    private static synchronized void mkdirs(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create directory " + dir);
            }
        }
    }

    @Override
    public Options getOptions() {
        return new Options();
    }

    @Override
    public boolean parseOptions(CommandLine line) {
        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        destinationDir = new File(destination);
        if (!destinationDir.exists())
            throw new IllegalArgumentException("destination directory must exist");
        if (!destinationDir.isDirectory())
            throw new IllegalArgumentException("destination must be a directory");
        if (!destinationDir.canWrite())
            throw new IllegalArgumentException("destination must be writable");
    }

    @Override
    public void validateChain(SyncPlugin first) {
    }

    @Override
    public String getName() {
        return "CuaFilesystem Destination";
    }

    @Override
    public String getDocumentation() {
        return "Custom destination for CUA clips written to CAS to be exported as files.";
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
