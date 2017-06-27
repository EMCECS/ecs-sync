package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.filter.DxExtractorConfig;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.cas.ClipSyncObject;
import com.emc.ecs.sync.storage.cas.ClipTag;

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
            Iterator<ClipTag> tagIterator = clipObject.getTags().iterator();

            // this is tag 0. we won't use it, so let's close it
            // (it would also be closed by SyncTask when the source object is closed)
            tagIterator.next().close();

            // this is tag 1
            ClipTag clipTag = tagIterator.next();

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
