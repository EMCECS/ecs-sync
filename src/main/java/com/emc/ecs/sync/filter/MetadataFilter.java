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

import com.emc.ecs.sync.config.filter.MetadataConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MetadataFilter extends AbstractFilter<MetadataConfig> {
    private static final Logger log = LoggerFactory.getLogger(MetadataFilter.class);

    private Map<String, String> metadata;
    private Map<String, String> listableMetadata;
    private Map<String, String> changeMetadataKeys;

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        metadata = mapFromParams(config.getAddMetadata());
        listableMetadata = mapFromParams(config.getAddListableMetadata());
        changeMetadataKeys = mapFromParams(config.getChangeMetadataKeys());
    }

    @Override
    public void filter(ObjectContext objectContext) {
        String sourceId = objectContext.getSourceSummary().getIdentifier();
        ObjectMetadata meta = objectContext.getObject().getMetadata();

        if (config.isRemoveAllUserMetadata()) {
            meta.getUserMetadata().clear();
        } else if (config.getRemoveMetadata() != null) {
            for (String key : config.getRemoveMetadata()) {
                meta.getUserMetadata().remove(key);
            }
        }

        for (String key : metadata.keySet()) {
            log.debug("adding metadata {}={} to {}", key, metadata.get(key), sourceId);
            meta.setUserMetadataValue(key, metadata.get(key));
        }

        for (String key : listableMetadata.keySet()) {
            log.debug("adding listable metadata {}={} to {}", key, listableMetadata.get(key), sourceId);
            meta.setUserMetadataValue(key, listableMetadata.get(key), true);
        }

        for (String oldKey : changeMetadataKeys.keySet()) {
            ObjectMetadata.UserMetadata oldMeta = meta.getUserMetadata().remove(oldKey);
            if (oldMeta == null) {
                log.debug("metadata key {} not found on {}", oldKey, sourceId);
            } else {
                log.debug("changing metadata key {} -> {} for {}", oldKey, changeMetadataKeys.get(oldKey), sourceId);
                meta.setUserMetadataValue(changeMetadataKeys.get(oldKey), oldMeta.getValue(), oldMeta.isIndexed());
            }
        }

        getNext().filter(objectContext);
    }

    // TODO: if verification ever includes metadata, revert metadata changes, here
    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    private Map<String, String> mapFromParams(String[] params) {
        Map<String, String> map = new HashMap<>();
        if (params != null) {
            for (String param : params) {
                String[] parts = param.split("=", 2);
                if (parts.length != 2) {
                    // Empty value?
                    map.put(parts[0], "");
                } else {
                    map.put(parts[0], parts[1]);
                }
            }
        }
        return map;
    }
}
