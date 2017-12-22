/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage.nfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.io.NfsFile;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.io.NfsFileOutputStream;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.sync.config.storage.NfsConfig;

/**
 * @author seibed
 *
 */
public class Nfs3Storage extends AbstractNfsStorage<NfsConfig, Nfs3, Nfs3File> {

    Nfs3 nfs = null;

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.nfs.AbstractNfsStorage#createInputStream(com.emc.ecs.nfsclient.nfs.io.NfsFile)
     */
    protected InputStream createInputStream(Nfs3File f) throws IOException {
        if (!f.exists()) {
            f.createNewFile();
        }
        return new NfsFileInputStream(f);
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.nfs.AbstractNfsStorage#createOutputStream(com.emc.ecs.nfsclient.nfs.io.NfsFile)
     */
    protected OutputStream createOutputStream(Nfs3File f) throws IOException {
        if (!f.exists()) {
            f.createNewFile();
        }
        return new NfsFileOutputStream(f);
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.nfs.AbstractNfsStorage#createFile(java.lang.String)
     */
    protected Nfs3File createFile(String identifier) throws IOException {
        String path = combineWithFileSeparator(config.getSubPath(), getRelativePath(identifier, true));
        return createFileFromPath(path);
    }

    /**
     * Get the nfs, creating it from the config if necessary.
     * 
     * @return the nfs
     */
    private Nfs3 getNfs() {
        if (nfs == null) {
            NfsConfig config = getConfig();
            try {
                nfs = new Nfs3(config.getServer() + ":" + config.getMountPath(), null, 3);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return nfs;
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.nfs.AbstractNfsStorage#createFile(com.emc.ecs.nfsclient.nfs.io.NfsFile, java.lang.String)
     */
    protected Nfs3File createFile(Nfs3File parent, String childName) throws IOException {
        return parent.getChildFile(childName);
    }

    /* (non-Javadoc)
     * @see com.emc.ecs.sync.storage.nfs.AbstractNfsStorage#createFileFromPath(java.lang.String)
     */
    @Override
    protected Nfs3File createFileFromPath(String path) throws IOException {
        if (!path.startsWith(NfsFile.separator)) {
            path = NfsFile.separator + path;
        }
        return new Nfs3File(getNfs(), path);
    }

}
