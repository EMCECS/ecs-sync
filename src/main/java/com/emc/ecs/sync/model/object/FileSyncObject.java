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
package com.emc.ecs.sync.model.object;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.util.FilesystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Scanner;

public class FileSyncObject extends AbstractSyncObject<File> {
    private static final Logger log = LoggerFactory.getLogger(FileSyncObject.class);

    private MimetypesFileTypeMap mimeMap;
    private boolean followLinks;
    private boolean symLink;
    private long overrideBytesRead = -1;

    public FileSyncObject(SyncPlugin parentPlugin, MimetypesFileTypeMap mimeMap, File sourceFile, String relativePath, boolean followLinks) {
        super(parentPlugin, sourceFile, sourceFile.getAbsolutePath(), relativePath, sourceFile.isDirectory());
        this.mimeMap = mimeMap;
        this.followLinks = followLinks;
        this.symLink = FilesystemUtil.isSymLink(sourceFile);
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
    public boolean isDirectory() {
        return super.isDirectory() && (followLinks || !symLink);
    }

    @Override
    public boolean isLargeObject(int threshold) {
        return (!isDirectory() && getRawSourceIdentifier().length() > threshold);
    }

    @Override
    public long getBytesRead() {
        if (overrideBytesRead > -1) return overrideBytesRead;
        else return super.getBytesRead();
    }

    @Override
    protected void loadObject() {
        File file = getRawSourceIdentifier();

        // first check for a "side-car" file for metadata from an object system
        if (!getParentPlugin().isIgnoreMetadata() && getMetaFile(file).exists()) {
            try {
                metadata = readMetadata(file);
            } catch (IOException e) {
                throw new RuntimeException("Could not read metadata file for " + file, e);
            }
            // otherwise collect filesystem metadata
        } else {
            metadata = FilesystemUtil.createFilesystemMetadata(file, mimeMap, getParentPlugin().isIncludeAcl(),
                    followLinks, true); // TODO: figure out "preserve"/"restore" option
        }

        // helpful logging for link visibility
        if (symLink) log.info("{} symbolic link {} -> {}",
                followLinks ? "following" : "storing", getRelativePath(), FilesystemUtil.getLinkTarget(file));
    }

    @Override
    public synchronized InputStream createSourceInputStream() {
        try {
            if (!followLinks && symLink) { // file is a link and we are *not* following links
                return new ByteArrayInputStream(new byte[0]);
            } else if (getRawSourceIdentifier().isDirectory()) {
                return null;
            } else {
                return new BufferedInputStream(createInputStream(getRawSourceIdentifier()), getParentPlugin().getBufferSize());
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not open source file:" + getRawSourceIdentifier(), e);
        }
    }

    @Override
    public void reset() {
        super.reset();
        overrideBytesRead = -1;
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
                log.warn("Failed to delete directory {}", file);
            }
        } else {
            boolean tryDelete = true;
            if (deleteOlderThan > 0) {
                if (System.currentTimeMillis() - file.lastModified() < deleteOlderThan) {
                    log.info("not deleting {}; it is not at least {} ms old", file, deleteOlderThan);
                    tryDelete = false;
                }
            }
            if (deleteCheckScript != null) {
                String[] args = new String[]{
                        deleteCheckScript.getAbsolutePath(),
                        file.getAbsolutePath()
                };
                try {
                    log.debug("delete check: " + Arrays.asList(args));
                    Process p = Runtime.getRuntime().exec(args);
                    while (true) {
                        try {
                            int exitCode = p.exitValue();

                            if (exitCode == 0) {
                                log.debug("delete check OK, exit code {}", exitCode);
                            } else {
                                log.info("delete check failed, exit code {}.  Not deleting file.", exitCode);
                                tryDelete = false;
                            }
                            break;
                        } catch (IllegalThreadStateException e) {
                            // Ignore.
                        }
                    }
                } catch (IOException e) {
                    log.info("error executing delete check script: {}.  Not deleting file.", e.toString());
                    tryDelete = false;
                }
            }
            if (tryDelete) {
                try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
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
                }
            }
        }
    }

    protected SyncMetadata readMetadata(File objectFile) throws IOException {
        try (InputStream is = new BufferedInputStream(createInputStream(getMetaFile(objectFile)))) {
            return SyncMetadata.fromJson(new Scanner(is).useDelimiter("\\A").next());
        }
    }

    protected File getMetaFile(File objectFile) {
        return createFile(SyncMetadata.getMetaPath(objectFile.getPath(), objectFile.isDirectory()));
    }

    public long getOverrideBytesRead() {
        return overrideBytesRead;
    }

    /**
     * Some targets may be more efficient with direct access to the file itself (as opposed to the generic stream
     * provided by AbstractSyncObject). In these cases, the target will manually set the number of bytes read here
     * so as not to affect the progress/stats of the overall operation
     */
    public void setOverrideBytesRead(long overrideBytesRead) {
        this.overrideBytesRead = overrideBytesRead;
    }
}
