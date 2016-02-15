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

import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.ObjectIdentifier;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.bean.ObjectInfo;
import com.emc.atmos.api.bean.ObjectMetadata;
import com.emc.atmos.api.bean.ReadObjectResponse;
import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.AtmosMetadata;
import com.emc.ecs.sync.source.AtmosSource;
import com.emc.ecs.sync.util.Function;
import com.emc.ecs.sync.util.TimingUtil;

import java.io.InputStream;

/**
 * Encapsulates the information needed for reading from Atmos and does
 * some lazy loading of data.
 */
public class AtmosSyncObject extends AbstractSyncObject<ObjectIdentifier> {
    private AtmosApi atmos;
    private Long size;

    public AtmosSyncObject(SyncPlugin parentPlugin, AtmosApi atmos, ObjectIdentifier sourceId, String relativePath) {
        this(parentPlugin, atmos, sourceId, relativePath, null);
    }

    public AtmosSyncObject(SyncPlugin parentPlugin, AtmosApi atmos, ObjectIdentifier sourceId, String relativePath, Long size) {
        super(parentPlugin, sourceId, sourceId.toString(), relativePath,
                sourceId instanceof ObjectPath && ((ObjectPath) sourceId).isDirectory());
        this.atmos = atmos;
        this.size = size;
    }

    @Override
    public boolean isLargeObject(int threshold) {
        return (!isDirectory() && size != null && size > threshold);
    }

    @Override
    public InputStream createSourceInputStream() {
        if (isDirectory()) return null;
        return TimingUtil.time(getParentPlugin(), AtmosSource.OPERATION_GET_OBJECT_STREAM, new Function<ReadObjectResponse<InputStream>>() {
            @Override
            public ReadObjectResponse<InputStream> call() {
                return atmos.readObjectStream(getRawSourceIdentifier(), null);
            }
        }).getObject();
    }

    // HEAD object in Atmos
    @Override
    protected void loadObject() {
        // deal with root of namespace
        if ("/".equals(getSourceIdentifier())) {
            this.metadata = new AtmosMetadata();
            return;
        }

        AtmosMetadata metadata = AtmosMetadata.fromObjectMetadata(TimingUtil.time(getParentPlugin(), AtmosSource.OPERATION_GET_ALL_META,
                new Function<ObjectMetadata>() {
                    @Override
                    public ObjectMetadata call() {
                        return atmos.getObjectMetadata(getRawSourceIdentifier());
                    }
                }));
        this.metadata = metadata;

        if (!isDirectory()) {
            // GET ?info will give use retention/expiration
            if (getParentPlugin().isIncludeRetentionExpiration()) {
                ObjectInfo info = TimingUtil.time(getParentPlugin(), AtmosSource.OPERATION_GET_OBJECT_INFO,
                        new Function<ObjectInfo>() {
                            @Override
                            public ObjectInfo call() {
                                return atmos.getObjectInfo(getRawSourceIdentifier());
                            }
                        });
                if (info.getRetention() != null) {
                    metadata.setRetentionEnabled(info.getRetention().isEnabled());
                    metadata.setRetentionEndDate(info.getRetention().getEndAt());
                }
                if (info.getExpiration() != null) {
                    metadata.setExpirationDate(info.getExpiration().getEndAt());
                }
            }
        }
    }
}
