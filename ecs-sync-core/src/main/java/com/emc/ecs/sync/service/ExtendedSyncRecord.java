/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import java.util.Date;

public class ExtendedSyncRecord extends SyncRecord {
    private String sourceMd5;
    private Date sourceRetentionEndTime;
    private Date targetMtime;
    private String targetMd5;
    private Date targetRetentionEndTime;
    private String firstErrorMessage;

    public String getSourceMd5() {
        return sourceMd5;
    }

    public void setSourceMd5(String sourceMd5) {
        this.sourceMd5 = sourceMd5;
    }

    public Date getSourceRetentionEndTime() {
        return sourceRetentionEndTime;
    }

    public void setSourceRetentionEndTime(Date sourceRetentionEndTime) {
        this.sourceRetentionEndTime = sourceRetentionEndTime;
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

    public String getFirstErrorMessage() {
        return firstErrorMessage;
    }

    public void setFirstErrorMessage(String firstErrorMessage) {
        this.firstErrorMessage = firstErrorMessage;
    }
}
