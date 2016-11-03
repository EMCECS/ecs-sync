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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.filter.MetadataFilterConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MetadataFilter extends AbstractFilter<MetadataFilterConfig> {
    private static final Logger log = LoggerFactory.getLogger(MetadataFilter.class);

    private Map<String, String> metadata;
    private Map<String, String> listableMetadata;

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        metadata = mapFromParams(config.getAddMetadata());
        listableMetadata = mapFromParams(config.getAddListableMetadata());
    }

    @Override
    public void filter(ObjectContext objectContext) {
        String sourceId = objectContext.getSourceSummary().getIdentifier();
        ObjectMetadata meta = objectContext.getObject().getMetadata();
        for (String key : metadata.keySet()) {
            log.debug(String.format("adding metadata %s=%s to %s", key, metadata.get(key), sourceId));
            meta.setUserMetadataValue(key, metadata.get(key));
        }

        for (String key : listableMetadata.keySet()) {
            log.debug(String.format("adding listable metadata %s=%s to %s", key, metadata.get(key), sourceId));
            meta.setUserMetadataValue(key, metadata.get(key), true);
        }

        getNext().filter(objectContext);
    }

    // TODO: if verification ever includes metadata, remove added metadata, here
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
