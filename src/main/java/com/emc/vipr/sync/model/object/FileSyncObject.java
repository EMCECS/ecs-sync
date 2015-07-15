/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.model.object;

import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.FilesystemUtil;
import com.emc.vipr.sync.util.SyncUtil;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Scanner;

public class FileSyncObject extends AbstractSyncObject<File> {
    private static final Logger l4j = Logger.getLogger(FileSyncObject.class);

    private SyncPlugin parentPlugin;
    private MimetypesFileTypeMap mimeMap;

    public FileSyncObject(SyncPlugin parentPlugin, MimetypesFileTypeMap mimeMap, File sourceFile, String relativePath) {
        super(sourceFile, sourceFile.getAbsolutePath(), relativePath, sourceFile.isDirectory());
        this.parentPlugin = parentPlugin;
        this.mimeMap = mimeMap;
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
    protected void loadObject() {
        // first check for a "side-car" file for metadata from an object system
        if (!parentPlugin.isIgnoreMetadata() && getMetaFile(getRawSourceIdentifier()).exists()) {
            try {
                metadata = readMetadata(getRawSourceIdentifier());
            } catch (IOException e) {
                throw new RuntimeException("Could not read metadata file for " + getRawSourceIdentifier(), e);
            }
            // otherwise collect filesystem metadata
        } else {
            metadata = FilesystemUtil.createFilesystemMetadata(getRawSourceIdentifier(), mimeMap, parentPlugin.isIncludeAcl());
        }
    }

    @Override
    public synchronized InputStream createSourceInputStream() {
        try {
            return getRawSourceIdentifier().isDirectory() ? null :
                    new BufferedInputStream(createInputStream(getRawSourceIdentifier()), parentPlugin.getBufferSize());
        } catch (IOException e) {
            throw new RuntimeException("Could not open source file:" + getRawSourceIdentifier(), e);
        }
    }

    public void delete(long deleteOlderThan, File deleteCheckScript) {
        File metaFile = getMetaFile(getRawSourceIdentifier());
        if (metaFile.exists()) delete(metaFile, deleteOlderThan, deleteCheckScript);
        delete(getRawSourceIdentifier(), deleteOlderThan, deleteCheckScript);
    }

    protected void delete(File file, long deleteOlderThan, File deleteCheckScript) {
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
                    SyncUtil.safeClose(raf);
                }
            }
        }
    }

    protected SyncMetadata readMetadata(File objectFile) throws IOException {
        InputStream is = null;
        try {
            is = new BufferedInputStream(createInputStream(getMetaFile(objectFile)));
            return SyncMetadata.fromJson(new Scanner(is).useDelimiter("\\A").next());
        } finally {
            SyncUtil.safeClose(is);
        }
    }

    protected File getMetaFile(File objectFile) {
        return createFile(SyncMetadata.getMetaPath(objectFile.getPath(), objectFile.isDirectory()));
    }

}
