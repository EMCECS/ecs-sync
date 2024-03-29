/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.filter.InternalFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * !!INTERNAL USE ONLY!!
 * <p>
 * This class is the bridge between the filter chain and the target storage. It includes logic to determine whether to
 * create or update in the target and also whether an update is necessary based on mtime and size.
 */
@InternalFilter
public class TargetFilter extends AbstractFilter {
    private static final Logger log = LoggerFactory.getLogger(TargetFilter.class);

    private SyncStorage target;

    public TargetFilter(SyncStorage target, SyncOptions options) {
        this.target = target;
        setOptions(options);
    }

    @Override
    public void filter(ObjectContext objectContext) {
        String targetId = objectContext.getTargetId();
        SyncObject sourceObj = objectContext.getObject();
        if (targetId == null) {
            targetId = target.getIdentifier(sourceObj.getRelativePath(), sourceObj.getMetadata().isDirectory());
            objectContext.setTargetId(targetId);
        }

        SyncObject targetObj = null;
        try {
            targetObj = target.loadObject(targetId);

            // if required, update object context with target mtime and retention end-date
            if (options.isDbEnhancedDetailsEnabled()) {
                objectContext.setTargetMtime(targetObj.getMetadata().getModificationTime());
                objectContext.setTargetRetentionEndTime(targetObj.getMetadata().getRetentionEndDate());
            }

            log.debug("calling beforeUpdate hook method for {}", sourceObj.getRelativePath());
            target.beforeUpdate(objectContext, targetObj);

            // object needs to be updated
            log.debug("updating object in target (source:{}, target:{})...",
                    objectContext.getSourceSummary().getIdentifier(), targetId);
            target.updateObject(targetId, sourceObj);
            log.debug("target object updated ({})", targetId);
        } catch (ObjectNotFoundException e) {

            // object doesn't exist; create it
            log.debug("creating object in target (source:{}, target:{})...",
                    objectContext.getSourceSummary().getIdentifier(), targetId);
            objectContext.setTargetId(target.createObject(sourceObj));
            log.debug("target object created ({})", objectContext.getTargetId());

            // if we are not verifying, this is the only place we can update the object context with
            // target mtime and retention end-date if that is required
            if (options.isDbEnhancedDetailsEnabled() && !options.isVerify()) {
                targetObj = target.loadObject(objectContext.getTargetId());
                objectContext.setTargetMtime(targetObj.getMetadata().getModificationTime());
                objectContext.setTargetRetentionEndTime(targetObj.getMetadata().getRetentionEndDate());
            }
        } finally {
            try {
                if (targetObj != null) targetObj.close();
            } catch (Throwable t) {
                log.warn("could not close target object (" + objectContext.getTargetId() + ")", t);
            }
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        String identifier = objectContext.getTargetId();
        if (identifier == null) {
            identifier = target.getIdentifier(objectContext.getObject().getRelativePath(), objectContext.getObject().getMetadata().isDirectory());
            objectContext.setTargetId(identifier);
        }
        SyncObject object = target.loadObject(identifier);

        // if required, update object context with target mtime and retention end-date
        if (options.isDbEnhancedDetailsEnabled()) {
            objectContext.setTargetMtime(object.getMetadata().getModificationTime());
            objectContext.setTargetRetentionEndTime(object.getMetadata().getRetentionEndDate());
        }

        return object;
    }
}
