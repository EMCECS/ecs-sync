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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.model.ObjectContext;

import java.io.Closeable;

public interface DbService extends Closeable {

    /**
     * Implementations must provide a method to wipe out their database. This may mean deleting a .db file (Sqlite) or
     * dropping relevant tables, etc. Be aware that this operation may be done after {@link #close()} is called
     */
    void deleteDatabase();

    boolean setStatus(ObjectContext context, String error, boolean newRow);

    boolean setDeleted(ObjectContext context, boolean newRow);

    SyncRecord getSyncRecord(ObjectContext context);

    Iterable<SyncRecord> getSyncErrors();

    String getObjectsTableName();

    void setObjectsTableName(String objectsTableName);

    int getMaxErrorSize();

    void setMaxErrorSize(int maxErrorSize);
}
