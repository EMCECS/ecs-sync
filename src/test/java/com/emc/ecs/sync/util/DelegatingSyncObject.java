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
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;

import java.io.InputStream;
import java.util.Map;

public abstract class DelegatingSyncObject extends SyncObject {
    protected SyncObject delegate;

    public DelegatingSyncObject(SyncObject delegate) {
        super(delegate.getSource(), delegate.getRelativePath(), delegate.getMetadata());
        this.delegate = delegate;
    }

    @Override
    public SyncStorage getSource() {
        return delegate.getSource();
    }

    @Override
    public String getRelativePath() {
        return delegate.getRelativePath();
    }

    @Override
    public ObjectMetadata getMetadata() {
        return delegate.getMetadata();
    }

    @Override
    public void setMetadata(ObjectMetadata metadata) {
        delegate.setMetadata(metadata);
    }

    @Override
    public InputStream getDataStream() {
        return delegate.getDataStream();
    }

    @Override
    public void setDataStream(InputStream dataStream) {
        delegate.setDataStream(dataStream);
    }

    @Override
    public ObjectAcl getAcl() {
        return delegate.getAcl();
    }

    @Override
    public void setAcl(ObjectAcl acl) {
        delegate.setAcl(acl);
    }

    @Override
    public boolean isPostStreamUpdateRequired() {
        return delegate.isPostStreamUpdateRequired();
    }

    @Override
    public void setPostStreamUpdateRequired(boolean postStreamUpdateRequired) {
        delegate.setPostStreamUpdateRequired(postStreamUpdateRequired);
    }

    @Override
    public Map<String, Object> getProperties() {
        return delegate.getProperties();
    }

    @Override
    public void setProperties(Map<String, Object> properties) {
        delegate.setProperties(properties);
    }

    @Override
    public Object getProperty(String name) {
        return delegate.getProperty(name);
    }

    @Override
    public void setProperty(String name, Object value) {
        delegate.setProperty(name, value);
    }

    @Override
    public void removeProperty(String name) {
        delegate.removeProperty(name);
    }

    @Override
    public long getBytesRead() {
        return delegate.getBytesRead();
    }

    @Override
    public String getMd5Hex(boolean forceRead) {
        return delegate.getMd5Hex(forceRead);
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
