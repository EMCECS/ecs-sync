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

import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.FileSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.FilesystemSource;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.FilesystemTarget;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.ConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LocalCacheFilter extends SyncFilter {
    private static final Logger log = LoggerFactory.getLogger(LocalCacheFilter.class);

    public static final String ACTIVATION_NAME = "local-cache";

    public static final String CACHE_ROOT_OPT = "cache-root";
    public static final String CACHE_ROOT_DESC = "specifies the root directory in which to cache files. required";
    public static final String CACHE_ROOT_ARG_NAME = "cache-directory";

    private File cacheRoot;
    private FilesystemTarget cacheTarget;
    private FilesystemSource cacheSource;

    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt(CACHE_ROOT_OPT).desc(CACHE_ROOT_DESC)
                .hasArg().argName(CACHE_ROOT_ARG_NAME).build());
        return options;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        if (line.hasOption(CACHE_ROOT_OPT))
            cacheRoot = new File(line.getOptionValue(CACHE_ROOT_OPT));
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (cacheRoot == null)
            throw new ConfigurationException("must specify a cache root");

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

        cacheTarget = new FilesystemTarget();
        cacheTarget.setTargetRoot(cacheRoot);
        cacheTarget.configure(source, filtersBeforeCache.iterator(), cacheTarget);

        cacheSource = new FilesystemSource();
        cacheSource.setRootFile(cacheRoot);
        cacheSource.configure(cacheSource, filtersAfterCache.iterator(), target);
    }

    @Override
    public void filter(SyncObject obj) {

        // write to local cache
        log.info("writing " + obj + " to local cache");
        cacheTarget.filter(obj);

        // re-create source
        File cacheFile = new File(obj.getTargetIdentifier());
        FileSyncObject cacheObj = new FileSyncObject(cacheSource, new MimetypesFileTypeMap(), cacheFile, obj.getRelativePath(), true);

        try {
            // send cached object to real target
            log.info("writing cache of " + obj + " to target");
            getNext().filter(cacheObj);

            // apply downstream changes to original object
            obj.setTargetIdentifier(cacheObj.getTargetIdentifier());
            obj.setMetadata(cacheObj.getMetadata());
        } finally {
            // try to clean up cache to limit disk usage
            // leave directories for post-sync cleanup since they will likely cause contention
            try {
                if (!cacheObj.isDirectory()) {
                    log.debug("deleting local cache of " + obj);
                    cacheSource.delete(cacheObj);
                }
            } catch (Throwable t) {
                log.warn("could not clean up cache object " + cacheObj);
            }
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        return getNext().reverseFilter(obj);
    }

    /**
     * Attempt to clean up the cache directory contents
     */
    @Override
    public void cleanup() {
        super.cleanup();
        log.info("cleaning up cache directory...");
        cleanup(cacheSource.iterator());
    }

    protected void cleanup(Iterator<FileSyncObject> iterator) {
        while (iterator.hasNext()) {
            FileSyncObject obj = iterator.next();
            if (obj.isDirectory()) {
                log.debug("cleaning up children of " + obj);
                cleanup(cacheSource.childIterator(obj));
            }
            log.debug("cleaning up " + obj);
            if (obj.getRelativePath().isEmpty()) {
                // don't delete the root cache dir, just make sure it's empty
                File metaFile = new File(SyncMetadata.getMetaPath(obj.getRawSourceIdentifier().getPath(), obj.isDirectory()));
                if (metaFile.exists() && !metaFile.delete())
                    log.warn("could not delete root cache dir meta file " + metaFile.getPath());
                else if (metaFile.getParentFile().exists() && !metaFile.getParentFile().delete())
                    log.warn("could not delete root cache meta directory " + metaFile.getParentFile().getPath());
            } else {
                cacheSource.delete(obj);
            }
        }
    }

    @Override
    public String getName() {
        return "Local Cache";
    }

    @Override
    public String getDocumentation() {
        return "Writes each object to a local cache directory before writing to the target. Useful for applying " +
                "external transformations or for transforming objects in-place (source/target are the same).";
    }

    public File getCacheRoot() {
        return cacheRoot;
    }

    public void setCacheRoot(File cacheRoot) {
        this.cacheRoot = cacheRoot;
    }
}
