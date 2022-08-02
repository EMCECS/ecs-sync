/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.config.RoleType;
import com.emc.ecs.sync.model.SyncObject;

public class OperationDetails {
    private RoleType role;
    private String operation;
    private SyncObject syncObject;
    private String identifier;
    private long durationMs;
    private RuntimeException exception;

    /**
     * The {@link RoleType} of the storage plugin that executed the operation.
     */
    public RoleType getRole() {
        return role;
    }

    public void setRole(RoleType role) {
        this.role = role;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    /**
     * If an applicable {@link SyncObject} was available at the time of the operation, it will be set here. Note that
     * this may be null.
     */
    public SyncObject getSyncObject() {
        return syncObject;
    }

    public void setSyncObject(SyncObject syncObject) {
        this.syncObject = syncObject;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    /**
     * The millisecond duration of the operation.
     */
    public long getDurationMs() {
        return durationMs;
    }

    public void setDurationMs(long durationMs) {
        this.durationMs = durationMs;
    }

    /**
     * If an exception occurred during the operation, it will be set here. Otherwise, this should be null.
     */
    public RuntimeException getException() {
        return exception;
    }

    public void setException(RuntimeException exception) {
        this.exception = exception;
    }

    public OperationDetails withRole(RoleType role) {
        setRole(role);
        return this;
    }

    public OperationDetails withOperation(String operation) {
        setOperation(operation);
        return this;
    }

    public OperationDetails withSyncObject(SyncObject syncObject) {
        setSyncObject(syncObject);
        return this;
    }

    public OperationDetails withIdentifier(String identifier) {
        setIdentifier(identifier);
        return this;
    }

    public OperationDetails withDurationMs(long durationMs) {
        setDurationMs(durationMs);
        return this;
    }

    public OperationDetails withException(RuntimeException exception) {
        setException(exception);
        return this;
    }
}
