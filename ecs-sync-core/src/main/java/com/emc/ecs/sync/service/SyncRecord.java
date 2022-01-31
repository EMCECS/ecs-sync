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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.model.ObjectStatus;

import java.util.Date;

public class SyncRecord {
    private String sourceId;
    private String targetId;
    private boolean directory;
    private long size;
    private Date mtime;
    private ObjectStatus status;
    private Date transferStart;
    private Date transferComplete;
    private Date verifyStart;
    private Date verifyComplete;
    private int retryCount;
    private String errorMessage;
    private boolean sourceDeleted;

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public boolean isDirectory() {
        return directory;
    }

    public void setDirectory(boolean directory) {
        this.directory = directory;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public Date getMtime() {
        return mtime;
    }

    public void setMtime(Date mtime) {
        this.mtime = mtime;
    }

    public ObjectStatus getStatus() {
        return status;
    }

    public void setStatus(ObjectStatus status) {
        this.status = status;
    }

    public Date getTransferStart() {
        return transferStart;
    }

    public void setTransferStart(Date transferStart) {
        this.transferStart = transferStart;
    }

    public Date getTransferComplete() {
        return transferComplete;
    }

    public void setTransferComplete(Date transferComplete) {
        this.transferComplete = transferComplete;
    }

    public Date getVerifyStart() {
        return verifyStart;
    }

    public void setVerifyStart(Date verifyStart) {
        this.verifyStart = verifyStart;
    }

    public Date getVerifyComplete() {
        return verifyComplete;
    }

    public void setVerifyComplete(Date verifyComplete) {
        this.verifyComplete = verifyComplete;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean isSourceDeleted() {
        return sourceDeleted;
    }

    public void setSourceDeleted(boolean sourceDeleted) {
        this.sourceDeleted = sourceDeleted;
    }
}
