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

import com.emc.ecs.sync.config.filter.PathShardingConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import org.apache.commons.codec.digest.DigestUtils;

public class PathShardingFilter extends AbstractFilter<PathShardingConfig> {
    public static final String PROP_ORIGINAL_PATH = "pathShardingFilter.originalPath";

    @Override
    public void filter(ObjectContext objectContext) {
        // capture and save original path as property
        String relPath = objectContext.getObject().getRelativePath();
        objectContext.getObject().setProperty(PROP_ORIGINAL_PATH, relPath);

        // calculate MD5 and prefix path with shards
        String md5Hex = DigestUtils.md5Hex(relPath);
        StringBuilder shardedPath = new StringBuilder();
        for (int i = 0; i < config.getShardCount(); i++) {
            int start = i * config.getShardSize();
            shardedPath.append(md5Hex.substring(start, start + config.getShardSize())).append("/");
        }
        shardedPath.append(relPath);
        objectContext.getObject().setRelativePath(shardedPath.toString());

        // must continue filter chain
        getNext().filter(objectContext);
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        SyncObject object = getNext().reverseFilter(objectContext);

        // replace path with original
        String origPath = (String) object.getProperty(PROP_ORIGINAL_PATH);
        if (origPath != null) object.setRelativePath(origPath);

        return object;
    }
}
