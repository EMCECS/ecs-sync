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
package com.emc.ecs.sync.model;

import com.emc.ecs.sync.config.SyncOptions;

import java.util.concurrent.atomic.AtomicInteger;

public class ObjectContext {
    private ObjectSummary sourceSummary;
    private String targetId;
    private SyncObject object;
    private ObjectStatus status;
    private AtomicInteger failures = new AtomicInteger();
    private SyncOptions options;

    public ObjectSummary getSourceSummary() {
        return sourceSummary;
    }

    public void setSourceSummary(ObjectSummary sourceSummary) {
        this.sourceSummary = sourceSummary;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public SyncObject getObject() {
        return object;
    }

    public void setObject(SyncObject object) {
        this.object = object;
    }

    public ObjectStatus getStatus() {
        return status;
    }

    public void setStatus(ObjectStatus status) {
        this.status = status;
    }

    public int getFailures() {
        return failures.intValue();
    }

    public void incFailures() {
        failures.incrementAndGet();
    }

    public SyncOptions getOptions() {
        return options;
    }

    public void setOptions(SyncOptions options) {
        this.options = options;
    }

    public ObjectContext withSourceSummary(ObjectSummary sourceSummary) {
        this.sourceSummary = sourceSummary;
        return this;
    }

    public ObjectContext withTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public ObjectContext withObject(SyncObject object) {
        this.object = object;
        return this;
    }

    public ObjectContext withStatus(ObjectStatus status) {
        this.status = status;
        return this;
    }

    public ObjectContext withOptions(SyncOptions options) {
        this.options = options;
        return this;
    }
}
