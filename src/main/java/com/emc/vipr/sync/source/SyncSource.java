/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.source;

import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncObject;

import java.util.Iterator;

/**
 * The base class for all source plugins.  Source plugins are iterators of root objects.  If the source system is
 * not hierarchical, then this will be all of the objects and none of them will have children.  If the source system is
 * hierarchical, then each SyncObject will be aware of whether it has children and the source plugin should be
 * able to yield those children.
 * <p/>
 * If you want to support source object deletion (via the --delete-source option or deleteSource property of ViPRSync),
 * override and implement the {@link #delete(com.emc.vipr.sync.model.SyncObject)} method.
 */
public abstract class SyncSource<T extends SyncObject> extends SyncPlugin implements Iterable<T> {
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
     * Override to add custom logic around syncing an individual object. Useful when sync objects have stateful
     * resources. Calling {@link com.emc.vipr.sync.filter.SyncFilter#filter(com.emc.vipr.sync.model.SyncObject)}
     * will initiate the sync operation for the object. The default implementation calls this method and does
     * nothing else. Be sure to let exceptions bubble to calling code so errors can be tracked/logged appropriately.
     *
     * @param syncObject  the object to be synced
     * @param filterChain the first filter in the filter chain that will process the sync operation. You must call
     *                    filterChain.filter(syncObject) at some point in this method to sync the object.
     */
    public void sync(T syncObject, SyncFilter filterChain) {
        filterChain.filter(syncObject);
    }

    /**
     * Implement to return the child objects of the specified syncObject. If
     * {@link com.emc.vipr.sync.model.SyncObject#isDirectory()} returns false, this method can return null.
     * Otherwise, it should return a valid iterator (which can be empty).
     */
    public abstract Iterator<T> childIterator(T syncObject);

    /**
     * Implement this method if you wish to support source object deletion after successful sync. This is a per-object
     * operation and is enabled by the --delete-source option or the deleteSource property of ViPRSync.
     */
    public void delete(T syncObject) {
    }

    @Override
    public String summarizeConfig() {
        StringBuilder summary = new StringBuilder(super.summarizeConfig());
        summary.append(" - sourceUri: ").append(sourceUri).append("\n");
        return summary.toString();
    }

    public String getSourceUri() {
        return sourceUri;
    }

    public void setSourceUri(String sourceUri) {
        this.sourceUri = sourceUri;
    }
}
