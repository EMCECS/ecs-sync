/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.storage.file;

import com.emc.ecs.sync.config.storage.ArchiveConfig;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileInputStream;
import net.java.truevfs.access.TFileOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

public class ArchiveStorage extends AbstractFilesystemStorage<ArchiveConfig> {
    private static final Logger log = LoggerFactory.getLogger(ArchiveStorage.class);

    @Override
    protected InputStream createInputStream(File f) throws IOException {
        return new TFileInputStream(new TFile(f));
    }

    @Override
    protected OutputStream createOutputStream(File f) throws IOException {
        return new TFileOutputStream(new TFile(f));
    }

    @Override
    public File createFile(String path) {
        return new TFile(path);
    }

    @Override
    public File createFile(File parent, String path) {
        return new TFile(parent, path);
    }

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        TFile tFile = new TFile(config.getPath());

        if (source == this) {
            if (!tFile.isArchive() || !tFile.isDirectory())
                throw new ConfigurationException("The source " + config.getPath() + " is not a valid archive. "
                        + "Note: tar files must fit entirely into memory and you will get this error if they are too large");
        } else {
            if (tFile.exists() && (!tFile.isArchive() || !tFile.isDirectory()))
                throw new ConfigurationException("The target " + config.getPath() + " exists and is not a valid archive. "
                        + "Note: tar files must fit entirely into memory and you will get this error if they are too large");
        }
    }

    @Override
    protected ObjectAcl readAcl(String identifier) {
        // unfortunately truevfs apparently doesn't offer a method of storing/retrieving POSIX ACLs
        return new ObjectAcl();
    }

    @Override
    protected void writeAcl(File file, ObjectAcl acl) {
        // unfortunately truevfs apparently doesn't offer a method of storing/retrieving POSIX ACLs
    }

    //TODO: make sure that the source object has not been modified since it was copied to the target, before deleting
    @Override
    public void delete(String identifier, SyncObject object) {
        // TODO: implement (low priority)
        log.warn("delete is not supported by this plugin");
    }
}
