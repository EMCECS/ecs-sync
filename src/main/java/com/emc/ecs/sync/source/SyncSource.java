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
package com.emc.ecs.sync.source;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.SyncEstimate;
import com.emc.ecs.sync.model.object.SyncObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The base class for all source plugins.  Source plugins are iterators of root objects.  If the source system is
 * not hierarchical, then this will be all of the objects and none of them will have children.  If the source system is
 * hierarchical, then each SyncObject will be aware of whether it has children and the source plugin should be
 * able to yield those children.
 * <p/>
 * If you want to support source object deletion (via the --delete-source option or deleteSource property of EcsSync),
 * override and implement the {@link #delete(SyncObject)} method.
 */
public abstract class SyncSource<T extends SyncObject> extends SyncPlugin implements Iterable<T> {
    private static final Logger log = LoggerFactory.getLogger(SyncSource.class);

    protected String sourceUri;

    /**
     * return true if this source implementation can handle the specified source parameter (passed on the command line)
     *
     * @param sourceUri the source URI passed on the command line (i.e.
     *                  "atmos:http://user:key@node1.company.com")
     * @return true if the plugin should be used to handle the specified source
     */
    public abstract boolean canHandleSource(String sourceUri);

    /**
     * Override if the implementation can provide an estimate of the total sync resources (number and size of objects,
     * etc.)  Default implementation returns null, signifying there is no estimate available
     */
    public SyncEstimate createEstimate() {
        return null;
    }

    /**
     * Override to veto objects under specific circumstances, i.e. to create a modified-since filter
     * TODO: replace the "veto" concept with a more natural construct
     */
    public boolean veto(T syncObject) {
        return false;
    }

    /**
     * Override to add custom logic around syncing an individual object. Useful when sync objects have stateful
     * resources. Calling {@link SyncFilter#filter(SyncObject)}
     * will initiate the sync operation for the object. The default implementation calls this method and does
     * nothing else. Be sure to let exceptions bubble to calling code so errors can be tracked/logged appropriately.
     *
     * @param syncObject  the object to be synced
     * @param filterChain the first filter in the filter chain that will process the sync operation. You must call
     *                    filterChain.filter(syncObject) at some point in this method to sync the object.
     */
    public void sync(T syncObject, SyncFilter filterChain) {
        try {
            filterChain.filter(syncObject);
        } finally {
            try {
                // be sure to close all object resources
                syncObject.close();
            } catch (Throwable t) {
                log.warn("could not close object resources", t);
            }
        }
    }

    /**
     * Implement to return the child objects of the specified syncObject. If
     * {@link SyncObject#isDirectory()} returns false, this method can return null.
     * Otherwise, it should return a valid iterator (which can be empty).
     */
    public abstract Iterator<T> childIterator(T syncObject);

    /**
     * Override this method if your plugin requires additional verification.
     */
    public void verify(final T syncObject, SyncFilter filterChain) {

        // this implementation only verifies data objects
        if (syncObject.isDirectory()) return;

        // get target object
        SyncObject targetObject = filterChain.reverseFilter(syncObject);

        try {
            verifyObjects(syncObject, targetObject);
        } finally {
            // make sure all object resources are closed
            try {
                syncObject.close();
            } catch (Throwable t) {
                log.warn("could not close source object resources", t);
            }
            try {
                targetObject.close();
            } catch (Throwable t) {
                log.warn("could not close target object resources", t);
            }
        }
    }

    protected void verifyObjects(final T sourceObject, final SyncObject targetObject) {

        // thread the streams for efficiency (in case of verify-only)
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<String> futureSourceMd5 = executor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return sourceObject.getMd5Hex(true);
            }
        });
        Future<String> futureTargetMd5 = executor.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return targetObject.getMd5Hex(true);
            }
        });
        executor.shutdown();

        try {
            String sourceMd5 = futureSourceMd5.get(), targetMd5 = futureTargetMd5.get();

            if (!sourceMd5.equals(targetMd5))
                throw new RuntimeException(String.format("MD5 sum mismatch (%s != %s)", sourceMd5, targetMd5));
            else
                log.debug("MD5 sum verified ({} == {})", sourceMd5, targetMd5);

        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException(e);
        }
    }

    /**
     * Implement this method if you wish to support source object deletion after successful sync. This is a per-object
     * operation and is enabled by the --delete-source option or the deleteSource property of EcsSync.
     */
    public void delete(T syncObject) {
        throw new UnsupportedOperationException(String.format("Delete is not supported by the %s plugin", getClass().getSimpleName()));
    }

    @Override
    public String summarizeConfig() {
        return super.summarizeConfig()
                + " - sourceUri: " + sourceUri + "\n";
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public void setSourceUri(String sourceUri) {
        this.sourceUri = sourceUri;
    }
}
