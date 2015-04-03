package com.emc.vipr.sync.model.object;

import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.ObjectIdentifier;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.bean.ObjectInfo;
import com.emc.atmos.api.bean.ObjectMetadata;
import com.emc.atmos.api.bean.ReadObjectResponse;
import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.source.AtmosSource;
import com.emc.vipr.sync.util.Function;
import com.emc.vipr.sync.util.TimingUtil;

import java.io.InputStream;

/**
 * Encapsulates the information needed for reading from Atmos and does
 * some lazy loading of data.
 */
public class AtmosSyncObject extends AbstractSyncObject<ObjectIdentifier> {
    private SyncPlugin parentPlugin;
    private AtmosApi atmos;

    public AtmosSyncObject(SyncPlugin parentPlugin, AtmosApi atmos, ObjectIdentifier sourceId, String relativePath) {
        super(sourceId, sourceId.toString(), relativePath,
                sourceId instanceof ObjectPath && ((ObjectPath) sourceId).isDirectory());
        this.parentPlugin = parentPlugin;
        this.atmos = atmos;
    }

    @Override
    public InputStream createSourceInputStream() {
        if (isDirectory()) return null;
        return TimingUtil.time(parentPlugin, AtmosSource.OPERATION_GET_OBJECT_STREAM, new Function<ReadObjectResponse<InputStream>>() {
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

        AtmosMetadata metadata = AtmosMetadata.fromObjectMetadata(TimingUtil.time(parentPlugin, AtmosSource.OPERATION_GET_ALL_META,
                new Function<ObjectMetadata>() {
                    @Override
                    public ObjectMetadata call() {
                        return atmos.getObjectMetadata(getRawSourceIdentifier());
                    }
                }));
        this.metadata = metadata;

        if (isDirectory()) {
            metadata.setSize(0);
        } else {
            // GET ?info will give use retention/expiration
            if (parentPlugin.isIncludeRetentionExpiration()) {
                ObjectInfo info = TimingUtil.time(parentPlugin, AtmosSource.OPERATION_GET_OBJECT_INFO,
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
