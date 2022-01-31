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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.CuaExtractorConfig;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.cas.CasStorage;
import com.emc.ecs.sync.storage.cas.ClipSyncObject;
import com.emc.ecs.sync.storage.cas.EnhancedTag;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

public class CuaExtractor extends AbstractExtractor<CuaExtractorConfig> {
    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (!(source instanceof CasStorage))
            throw new ConfigurationException("CUA Extractor can only be used with a CAS Source");
    }

    @Override
    protected InputStream getDataStream(SyncObject originalObject) {
        if (!(originalObject instanceof ClipSyncObject))
            throw new UnsupportedOperationException("data object was not a CAS clip");

        final ClipSyncObject clipObject = (ClipSyncObject) originalObject;

        // CUA has blob data in tag 0
        EnhancedTag clipTag = clipObject.getTags().iterator().next();

        try {
            if (clipTag.isBlobAttached()) return clipTag.getBlobInputStream();
                // if there is no blob data, we can only assume a zero-byte object
                // TODO: we need to verify this somehow; if this is wrong, it will never show up in a migration
            else return new ByteArrayInputStream(new byte[0]);
        } catch (Exception e) {
            throw new RuntimeException("could not get CUA file data", e);
        }
    }
}
