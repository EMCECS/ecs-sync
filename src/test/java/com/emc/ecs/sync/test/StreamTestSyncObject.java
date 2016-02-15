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
package com.emc.ecs.sync.test;

import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.AbstractSyncObject;

import java.io.InputStream;

public class StreamTestSyncObject extends AbstractSyncObject<String> {
    InputStream inputStream;
    long size;

    public StreamTestSyncObject(String identifier, String relativePath, InputStream inputStream, long size) {
        super(null, identifier, identifier, relativePath, false);
        this.inputStream = inputStream;
        this.size = size;
    }

    @Override
    protected void loadObject() {
        if (metadata == null) metadata = new SyncMetadata();
        metadata.setContentLength(size);
    }

    @Override
    protected InputStream createSourceInputStream() {
        return inputStream;
    }
}
