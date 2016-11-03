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

import com.emc.ecs.sync.config.filter.OverrideMimetypeConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.config.ConfigurationException;

import java.util.Iterator;

/**
 * @author cwikj
 */
public class OverrideMimetypeFilter extends AbstractFilter<OverrideMimetypeConfig> {
    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (config.getOverrideMimetype() == null)
            throw new ConfigurationException("you must provide a mimetype");
    }

    @Override
    public void filter(ObjectContext objectContext) {
        ObjectMetadata metadata = objectContext.getObject().getMetadata();
        if (config.isForceMimetype()) {
            metadata.setContentType(config.getOverrideMimetype());
        } else {
            if (metadata.getContentType() == null || metadata.getContentType().equals("application/octet-stream")) {
                metadata.setContentType(config.getOverrideMimetype());
            }
        }

        getNext().filter(objectContext);
    }

    // TODO: if verification ever includes mime-type, reverse mime type
    // -- (how to keep track of old values to revert?)
    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }
}
