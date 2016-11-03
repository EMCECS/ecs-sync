/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.storage.SyncStorage;

import java.util.Iterator;

public interface SyncPlugin<C> extends AutoCloseable {
    /**
     * If this plugin is to be included in a sync, this method will be
     * called just before processing starts.  The source, target and filter plugins
     * will be established at this point.  This gives each plugin a chance to
     * configure itself, inspect all of the other plugins in the chain and throw
     * ConfigurationException if any errors or incompatibilities are discovered.
     * For instance, if this plugin requires the <code>rootFile</code> option and
     * it is not present, it should throw an exception here.
     * Also, if this plugin only supports Atmos targets in object mode, it could
     * validate that the target is an AtmosTarget and that the
     * target's namespaceRoot is null.  Any exception thrown here will stop the sync
     * from running.
     * <p>
     * When overriding this method, be sure to call super.configure() as well!
     */
    void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target);

    /**
     * Implement to perform any necessary cleanup logic after the entire sync is complete (i.e. close file handles,
     * streams, DB connections, etc.) NOTE: be sure to call super if you override this!
     */
    void close();

    C getConfig();

    void setConfig(C config);

    SyncOptions getOptions();

    void setOptions(SyncOptions options);
}
