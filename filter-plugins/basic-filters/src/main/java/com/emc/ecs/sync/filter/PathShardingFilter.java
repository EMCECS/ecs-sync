/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.PathShardingConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

public class PathShardingFilter extends AbstractFilter<PathShardingConfig> {
    public static final String PROP_ORIGINAL_PATH = "pathShardingFilter.originalPath";

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        // make sure we have an MD5 digest implementation
        try {
            MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new ConfigurationException("No MD5 digest implementation found", e);
        }
    }

    @Override
    public void filter(ObjectContext objectContext) {
        // capture and save original path as property
        String relPath = objectContext.getObject().getRelativePath();
        objectContext.getObject().setProperty(PROP_ORIGINAL_PATH, relPath);

        // calculate MD5 and prefix path with shard
        try {
            byte[] md5 = MessageDigest.getInstance("MD5").digest(relPath.getBytes(StandardCharsets.UTF_8));
            String md5Hex = DatatypeConverter.printHexBinary(md5);
            // fix case
            md5Hex = config.isUpperCase() ? md5Hex.toUpperCase() : md5Hex.toLowerCase();
            StringBuilder shardedPath = new StringBuilder();
            for (int i = 0; i < config.getShardCount(); i++) {
                int start = i * config.getShardSize();
                shardedPath.append(md5Hex, start, start + config.getShardSize()).append("/");
            }
            shardedPath.append(relPath);
            objectContext.getObject().setRelativePath(shardedPath.toString());

            // must continue filter chain
            getNext().filter(objectContext);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error loading MD5 digest", e); // this should not happen because we check it in configure()
        }
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
