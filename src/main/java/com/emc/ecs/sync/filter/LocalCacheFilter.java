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

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.LocalCacheConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.file.FilesystemStorage;
import com.emc.ecs.sync.storage.SyncStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LocalCacheFilter extends AbstractFilter<LocalCacheConfig> {
    private static final Logger log = LoggerFactory.getLogger(LocalCacheFilter.class);

    private FilesystemStorage cacheTarget;
    private FilesystemStorage cacheSource;

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (config.getLocalCacheRoot() == null)
            throw new ConfigurationException("must specify a cache root");

        File cacheRoot = new File(config.getLocalCacheRoot());
        if (!cacheRoot.exists() || !cacheRoot.isDirectory() || !cacheRoot.canWrite() || cacheRoot.list().length != 0)
            throw new ConfigurationException(cacheRoot + " is not a writable empty directory");

        // split plugin chain for each side of the cache (source -> cache and cache -> target)
        List<SyncFilter> filtersBeforeCache = new ArrayList<>();
        List<SyncFilter> filtersAfterCache = new ArrayList<>();
        boolean beforeCache = true;
        while (filters.hasNext()) {
            SyncFilter filter = filters.next();
            if (filter == this) beforeCache = false;
            else if (beforeCache) filtersBeforeCache.add(filter);
            else filtersAfterCache.add(filter);
        }

        FilesystemConfig cacheConfig = new FilesystemConfig();
        cacheConfig.setPath(config.getLocalCacheRoot());

        cacheTarget = new FilesystemStorage();
        cacheTarget.setConfig(cacheConfig);
        cacheTarget.setOptions(options);
        cacheTarget.configure(source, filtersBeforeCache.iterator(), cacheTarget);

        cacheSource = new FilesystemStorage();
        cacheSource.setConfig(cacheConfig);
        cacheSource.setOptions(options);
        cacheSource.configure(cacheSource, filtersAfterCache.iterator(), target);
    }

    @Override
    public void filter(ObjectContext objectContext) {
        SyncObject obj = objectContext.getObject();

        // write to local cache
        log.info("writing " + obj.getRelativePath() + " to local cache");
        String cacheId = cacheTarget.createObject(obj);

        // create cache source
        SyncObject cacheObj = cacheSource.loadObject(cacheId);

        // set original metadata and ACL
        cacheObj.setMetadata(obj.getMetadata());
        cacheObj.setAcl(obj.getAcl());

        // set the cached object in the context
        objectContext.setObject(cacheObj);

        try {
            // send cached object to real target
            log.info("writing cache of " + obj.getRelativePath() + " to target");
            getNext().filter(objectContext);
        } finally {
            // try to clean up cache to limit disk usage
            // leave directories for post-sync cleanup since they will likely cause contention
            try {
                if (!obj.getMetadata().isDirectory()) {
                    log.debug("deleting local cache of " + obj.getRelativePath());
                    cacheSource.delete(cacheId);
                }
            } catch (Throwable t) {
                log.warn("could not clean up cache object " + cacheId, t);
            }

            // set original object in context in case upstream plugins require it
            objectContext.setObject(obj);
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    /**
     * Attempt to clean up the cache directory contents
     */
    @Override
    public void close() {
        try (SyncStorage<?> source = cacheSource;
             SyncStorage<?> target = cacheTarget) {
            try {
                super.close();
            } catch (Throwable t) {
                //ignore
            }
            log.info("cleaning up cache directory...");
            delete(source.allObjects());
        }
    }

    protected void delete(Iterable<ObjectSummary> summaries) {
        for (ObjectSummary summary : summaries) {
            if (summary.isDirectory()) {
                log.debug("cleaning up children of " + summary.getIdentifier());
                delete(cacheSource.children(summary));
            }
            log.debug("cleaning up " + summary.getIdentifier());
            String relativePath = cacheSource.getRelativePath(summary.getIdentifier(), summary.isDirectory());
            if (relativePath.isEmpty()) {
                // don't delete the root cache dir, just make sure it's empty
                File metaFile = new File(ObjectMetadata.getMetaPath(relativePath, summary.isDirectory()));
                if (metaFile.exists() && !metaFile.delete())
                    log.warn("could not delete root cache dir meta file " + metaFile.getPath());
                else if (metaFile.getParentFile().exists() && !metaFile.getParentFile().delete())
                    log.warn("could not delete root cache meta directory " + metaFile.getParentFile().getPath());
            } else {
                cacheSource.delete(summary.getIdentifier());
            }
        }
    }
}
