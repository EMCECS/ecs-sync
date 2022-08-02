/*
 * Copyright (c) 2017-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.filter.DxExtractorConfig;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.cas.ClipSyncObject;
import com.emc.ecs.sync.storage.cas.EnhancedTag;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

public class DxExtractor extends AbstractExtractor<DxExtractorConfig> {
    @Override
    protected InputStream getDataStream(SyncObject originalObject) {

        if (originalObject instanceof ClipSyncObject) {
            // CAS source

            final ClipSyncObject clipObject = (ClipSyncObject) originalObject;

            // DX has blob data in tag 1
            Iterator<EnhancedTag> tagIterator = clipObject.getTags().iterator();

            // this is tag 0. we won't use it, so let's close it
            // (it would also be closed by SyncTask when the source object is closed)
            tagIterator.next().close();

            // this is tag 1
            EnhancedTag clipTag = tagIterator.next();

            try {
                if (clipTag.isBlobAttached()) return clipTag.getBlobInputStream();
                    // if there is no blob data, we can only assume a zero-byte object
                    // TODO: we need to verify this somehow; if this is wrong, it will never show up in a migration
                else return new ByteArrayInputStream(new byte[0]);
            } catch (Exception e) {
                throw new RuntimeException("could not get CUA file data", e);
            }

        } else {
            // non-CAS source

            return originalObject.getDataStream();
        }
    }
}
