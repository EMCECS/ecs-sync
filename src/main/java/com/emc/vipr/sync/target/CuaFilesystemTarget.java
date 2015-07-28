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
package com.emc.vipr.sync.target;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.ClipSyncObject;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.CasUtil;
import com.emc.vipr.sync.util.ClipTag;
import com.emc.vipr.sync.util.SyncUtil;
import com.filepool.fplibrary.FPTag;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Iterator;

/**
 * Plugin to extract files from a CUA (written to CAS) and replicate the source path structure on a local filesystem.
 */
public class CuaFilesystemTarget extends SyncTarget {
    private static final Logger l4j = Logger.getLogger(CuaFilesystemTarget.class);

    private static final String CLIP_NAME = "Storigen_File_Gateway";
    private static final String ATTRIBUTE_TAG_NAME = "Storigen_File_Gateway_File_Attributes";
    private static final String BLOB_TAG_NAME = "Storigen_File_Gateway_Blob";

    private static final String PATH_ATTRIBUTE = "Storigen_File_Gateway_File_Path0";
    private static final String ITIME_ATTRIBUTE = "Storigen_File_Gateway_File_CTime"; // Windows ctime means create time
    private static final String MTIME_ATTRIBUTE = "Storigen_File_Gateway_File_MTime";
    private static final String ATIME_ATTRIBUTE = "Storigen_File_Gateway_File_ATime";
    private static final String SIZE_HI_ATTRIBUTE = "Storigen_File_Gateway_File_Size_Hi";
    private static final String SIZE_LO_ATTRIBUTE = "Storigen_File_Gateway_File_Size_Lo";

    private String target;
    private File targetDir;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return false; // this plug-in is not CLI capable
    }

    @Override
    public Options getCustomOptions() {
        return new Options();
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        targetDir = new File(this.target);
        if (!targetDir.exists())
            throw new IllegalArgumentException("target directory must exist");
        if (!targetDir.isDirectory())
            throw new IllegalArgumentException("target must be a directory");
        if (!targetDir.canWrite())
            throw new IllegalArgumentException("target must be writable");
    }

    @Override
    public void filter(SyncObject obj) {
        timeOperationStart(CasUtil.OPERATION_TOTAL);

        if (!(obj instanceof ClipSyncObject))
            throw new UnsupportedOperationException("sync object was not a CAS clip");
        final ClipSyncObject clipSync = (ClipSyncObject) obj;

        try {
            // looking for clips with a specific name
            if (!clipSync.getClipName().equals(CLIP_NAME)) {
                LogMF.debug(l4j, "skipped clip {0} (clip name did not match)", clipSync.getRawSourceIdentifier());
            } else {

                ClipTag blobTag = null;
                String filePath = null;
                Date itime = null, mtime = null, atime = null;
                long hi = 0, lo = 0;
                for (ClipTag clipTag : clipSync.getTags()) {
                    FPTag sourceTag = clipTag.getTag();
                    if (sourceTag.getTagName().equals(ATTRIBUTE_TAG_NAME)) {
                        // pull all pertinent attributes
                        filePath = sourceTag.getStringAttribute(PATH_ATTRIBUTE);
                        itime = new Date(sourceTag.getLongAttribute(ITIME_ATTRIBUTE) * 1000); // tag value is in seconds
                        mtime = new Date(sourceTag.getLongAttribute(MTIME_ATTRIBUTE) * 1000); // .. convert to ms
                        atime = new Date(sourceTag.getLongAttribute(ATIME_ATTRIBUTE) * 1000);
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

                File destFile = new File(targetDir, filePath);
                obj.setTargetIdentifier(destFile.getPath());

                // transfer the clip if:
                // - force is enabled
                // - target does not exist
                // - source mtime is after target mtime, or
                // - source size is different from target size
                long size = (lo > 0 ? hi << 32 : hi) + lo;
                if (force || !destFile.exists() || mtime.after(new Date(destFile.lastModified())) || size != destFile.length()) {
                    LogMF.info(l4j, "writing {0} to {1}", obj.getSourceIdentifier(), destFile);

                    // make parent directories
                    mkdirs(destFile.getParentFile());

                    // write the file
                    OutputStream out = new FileOutputStream(destFile);
                    try {
                        blobTag.writeToStream(out);
                    } finally {
                        SyncUtil.safeClose(out);
                    }


                    // set times
                    LogMF.debug(l4j, "updating timestamps for {0}", destFile.getPath());
                    // setting create and access requires JDK 7
//                    Path destPath = destFile.toPath();
//                    Files.setAttribute(destPath, "creationTime", FileTime.fromMillis(itime.getTime()));
//                    Files.setAttribute(destPath, "lastModifiedTime", FileTime.fromMillis(mtime.getTime()));
//                    Files.setAttribute(destPath, "lastAccessTime", FileTime.fromMillis(atime.getTime()));
                    if (!destFile.setLastModified(mtime.getTime()))
                        LogMF.warn(l4j, "could not set mtime for {0}", destFile.getPath());

                    // verify size
                    if (size != destFile.length())
                        throw new RuntimeException(String.format("target file %s is not the right size (expected %d)", destFile, size));

                } else {
                    LogMF.info(l4j, "{0} is up to date, skipping", destFile);
                }
            }

            timeOperationComplete(CasUtil.OPERATION_TOTAL);
        } catch (Throwable t) {
            timeOperationFailed(CasUtil.OPERATION_TOTAL);
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            throw new RuntimeException("failed to sync object: " + t.getMessage(), t);
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not yet support reverse filters (verification)");
    }

    /**
     * Synchronized mkdir to prevent conflicts in threaded environment.
     */
    private static synchronized void mkdirs(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("failed to create directory " + dir);
            }
        }
    }

    @Override
    public String getName() {
        return "CuaFilesystem Target";
    }

    @Override
    public String getDocumentation() {
        return "Custom target for CUA clips written to CAS to be exported as files.";
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }
}
