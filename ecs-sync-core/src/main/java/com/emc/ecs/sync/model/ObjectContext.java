/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.model;

import com.emc.ecs.sync.config.SyncOptions;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectContext {
    private ObjectSummary sourceSummary;
    private SyncObject object;
    private String targetId;
    private Date targetMtime;
    private String targetMd5;
    private Date targetRetentionEndTime;
    private ObjectStatus status;
    private AtomicInteger failures = new AtomicInteger();
    private SyncOptions options;

    public ObjectSummary getSourceSummary() {
        return sourceSummary;
    }

    public void setSourceSummary(ObjectSummary sourceSummary) {
        this.sourceSummary = sourceSummary;
    }

    public SyncObject getObject() {
        return object;
    }

    public void setObject(SyncObject object) {
        this.object = object;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public Date getTargetMtime() {
        return targetMtime;
    }

    public void setTargetMtime(Date targetMtime) {
        this.targetMtime = targetMtime;
    }

    public String getTargetMd5() {
        return targetMd5;
    }

    public void setTargetMd5(String targetMd5) {
        this.targetMd5 = targetMd5;
    }

    public Date getTargetRetentionEndTime() {
        return targetRetentionEndTime;
    }

    public void setTargetRetentionEndTime(Date targetRetentionEndTime) {
        this.targetRetentionEndTime = targetRetentionEndTime;
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

    public ObjectContext withObject(SyncObject object) {
        this.object = object;
        return this;
    }

    public ObjectContext withTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public ObjectContext withTargetMtime(Date targetMtime) {
        this.targetMtime = targetMtime;
        return this;
    }

    public ObjectContext withTargetMd5(String targetMd5) {
        this.targetMd5 = targetMd5;
        return this;
    }

    public ObjectContext withTargetRetentionEndTime(Date targetRetentionEndTime) {
        this.targetRetentionEndTime = targetRetentionEndTime;
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
