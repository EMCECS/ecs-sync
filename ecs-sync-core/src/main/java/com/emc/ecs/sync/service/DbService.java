/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.model.ObjectContext;

import java.io.Closeable;

public interface DbService extends Closeable {

    /**
     * Implementations must provide a method to wipe out their database. This may mean deleting a .db file (Sqlite) or
     * dropping relevant tables, etc. Be aware that this operation may be done after {@link #close()} is called
     */
    void deleteDatabase();

    /**
     * Puts a lock on a source identifier. If the identifier is already locked, waits until unlocked. This is to avoid
     * duplicate insertion errors
     */
    void lock(String identifier);

    /**
     * Removes the lock on a source identifier. Be sure to unlock in a finally block
     */
    void unlock(String identifier);

    boolean setStatus(ObjectContext context, String error, boolean newRow);

    boolean setDeleted(ObjectContext context, boolean newRow);

    SyncRecord getSyncRecord(ObjectContext context);

    <T extends SyncRecord> Iterable<T> getAllRecords();

    <T extends SyncRecord> Iterable<T> getSyncErrors();

    <T extends SyncRecord> Iterable<T> getSyncRetries();

    String getObjectsTableName();

    void setObjectsTableName(String objectsTableName);

    int getMaxErrorSize();

    void setMaxErrorSize(int maxErrorSize);

    boolean isExtendedFieldsEnabled();
}
