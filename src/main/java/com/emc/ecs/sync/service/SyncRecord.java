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
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Expects the following schema:
 * <table>
 * <tr><td><code>source_id*</code></td></tr>
 * <tr><td><code>target_id</code></td></tr>
 * <tr><td><code>is_directory</code></td></tr>
 * <tr><td><code>size</code></td></tr>
 * <tr><td><code>mtime</code></td></tr>
 * <tr><td><code>status</code></td></tr>
 * <tr><td><code>transfer_start</code></td></tr>
 * <tr><td><code>transfer_complete</code></td></tr>
 * <tr><td><code>verify_start</code></td></tr>
 * <tr><td><code>verify_complete</code></td></tr>
 * <tr><td><code>retry_count</code></td></tr>
 * <tr><td><code>error_message</code></td></tr>
 * <tr><td><code>is_source_deleted</code></td></tr>
 * </table>
 * * primary key
 */
public class SyncRecord {
    public static final String SOURCE_ID = "source_id";
    public static final String TARGET_ID = "target_id";
    public static final String IS_DIRECTORY = "is_directory";
    public static final String SIZE = "size";
    public static final String MTIME = "mtime";
    public static final String STATUS = "status";
    public static final String TRANSFER_START = "transfer_start";
    public static final String TRANSFER_COMPLETE = "transfer_complete";
    public static final String VERIFY_START = "verify_start";
    public static final String VERIFY_COMPLETE = "verify_complete";
    public static final String RETRY_COUNT = "retry_count";
    public static final String ERROR_MESSAGE = "error_message";
    public static final String IS_SOURCE_DELETED = "is_source_deleted";

    public static final List<String> ALL_FIELDS = Arrays.asList(
            SOURCE_ID, TARGET_ID, IS_DIRECTORY, SIZE, MTIME, STATUS, TRANSFER_START, TRANSFER_COMPLETE,
            VERIFY_START, VERIFY_COMPLETE, RETRY_COUNT, ERROR_MESSAGE, IS_SOURCE_DELETED
    );

    /**
     * passing no fields will insert all fields
     */
    public static String insert(String tableName, String... fields) {
        String insert = "insert into " + tableName + " (";
        List<String> insertFields = new ArrayList<>(ALL_FIELDS);
        if (fields != null && fields.length > 0) insertFields = Arrays.asList(fields);
        insert += StringUtils.collectionToCommaDelimitedString(insertFields);
        insert += ") values (";
        for (int i = 0; i < insertFields.size(); i++) {
            insert += "?";
            if (i < insertFields.size() - 1) insert += ", ";
        }
        insert += ")";
        return insert;
    }

    public static String selectBySourceId(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(ALL_FIELDS)
                + " from " + tableName + " where " + SOURCE_ID + " = ?";
    }

    public static String selectAll(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(ALL_FIELDS)
                + " from " + tableName;
    }

    public static String selectErrors(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(ALL_FIELDS)
                + " from " + tableName + " where status = '" + ObjectStatus.Error.getValue() + "'";
    }

    public static String selectRetries(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(ALL_FIELDS)
                + " from " + tableName + " where status = '" + ObjectStatus.RetryQueue.getValue() + "'";
    }

    /**
     * passing no fields will update all fields except source_id
     */
    public static String updateBySourceId(String tableName, String... fields) {
        String update = "update " + tableName + " set ";
        List<String> updateFields = new ArrayList<>(ALL_FIELDS);
        updateFields.remove(SOURCE_ID);
        if (fields != null && fields.length > 0) updateFields = Arrays.asList(fields);
        for (int i = 0; i < updateFields.size(); i++) {
            update += updateFields.get(i) + "=?";
            if (i < updateFields.size() - 1) update += ", ";
        }
        update += " where " + SOURCE_ID + " = ?";
        return update;
    }

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

    public void setIsDirectory(boolean isDirectory) {
        this.directory = isDirectory;
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
