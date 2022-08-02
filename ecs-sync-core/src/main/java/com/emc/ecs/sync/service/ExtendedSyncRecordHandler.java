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

import com.emc.ecs.sync.model.ObjectContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 * <tr><td><code>source_md5</code></td></tr>
 * <tr><td><code>source_retention_end_time</code></td></tr>
 * <tr><td><code>target_mtime</code></td></tr>
 * <tr><td><code>target_md5</code></td></tr>
 * <tr><td><code>target_retention_end_time</code></td></tr>
 * <tr><td><code>first_error_message</code></td></tr>
 * </table>
 * * primary key
 */
public class ExtendedSyncRecordHandler extends SyncRecordHandler {
    private static final Logger log = LoggerFactory.getLogger(ExtendedSyncRecordHandler.class);

    public static final DbField SOURCE_MD5 = DbField.create("source_md5", DbField.Type.string, 32, true);
    public static final DbField SOURCE_RETENTION_END_TIME = DbField.create("source_retention_end_time", DbField.Type.datetime, -1, true);
    public static final DbField TARGET_MTIME = DbField.create("target_mtime", DbField.Type.datetime, -1, true);
    public static final DbField TARGET_MD5 = DbField.create("target_md5", DbField.Type.string, 32, true);
    public static final DbField TARGET_RETENTION_END_TIME = DbField.create("target_retention_end_time", DbField.Type.datetime, -1, true);
    public static final DbField FIRST_ERROR_MESSAGE = DbField.create("first_error_message", DbField.Type.string, -1, true);

    public static final List<DbField> EXTENDED_FIELDS = Collections.unmodifiableList(Arrays.asList(
            SOURCE_MD5, SOURCE_RETENTION_END_TIME, TARGET_MTIME, TARGET_MD5, TARGET_RETENTION_END_TIME, FIRST_ERROR_MESSAGE
    ));

    public static final List<DbField> ALL_FIELDS = Collections.unmodifiableList(
            Stream.concat(SyncRecordHandler.ALL_FIELDS.stream(), EXTENDED_FIELDS.stream()).collect(Collectors.toList())
    );

    public static final List<DbField> ERROR_FIELDS = Collections.unmodifiableList(Arrays.asList(
            ERROR_MESSAGE, FIRST_ERROR_MESSAGE
    ));

    /**
     * passing no fields will insert all fields
     */
    public static String insert(String tableName, DbParams params) {
        if (params == null) {
            params = DbParams.create();
            for (DbField field : ALL_FIELDS) {
                params.addDataParam(field, null);
            }
        }
        return SyncRecordHandler.insert(tableName, params);
    }

    /**
     * passing no fields will update all fields except source_id
     */
    public static String updateBySourceId(String tableName, DbParams params) {
        if (params == null) {
            params = DbParams.create();
            for (DbField field : ALL_FIELDS) {
                if (field != SOURCE_ID) params.addDataParam(field, null);
            }
        }
        return SyncRecordHandler.updateBySourceId(tableName, params);
    }

    public ExtendedSyncRecordHandler(int maxErrorSize, SqlDateMapper dateMapper) {
        super(maxErrorSize, dateMapper);
    }

    @Override
    protected List<DbField> allFields() {
        return ALL_FIELDS;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends SyncRecord> RowMapper<T> mapper() {
        return (RowMapper<T>) new Mapper();
    }

    @Override
    public DbParams insertStatusParams(ObjectContext context, String error) {
        DbParams params = super.insertStatusParams(context, error);
        String sourceMd5 = null, targetMd5 = context.getTargetMd5();
        Date sourceRetentionEndTime = null;
        try {
            sourceRetentionEndTime = context.getObject().getMetadata().getRetentionEndDate();
            try {
                if (context.getStatus().isSuccess()) sourceMd5 = context.getObject().getMd5Hex(false);
                // we only want to store standard (non-aggregated) MD5 values (the column is only sized for 32 chars)
                if (sourceMd5 != null && sourceMd5.length() > 32) sourceMd5 = null;
            } catch (Throwable t) {
                log.info("could not get source MD5 for object {}: {}", context.getSourceSummary().getIdentifier(), t.toString());
            }
        } catch (Throwable t) {
            log.info("could not pull metadata from object {}: {}", context.getSourceSummary().getIdentifier(), t.toString());
        }
        if (targetMd5 != null && targetMd5.length() > 32) targetMd5 = null;

        if (sourceMd5 != null) params.addDataParam(SOURCE_MD5, sourceMd5);
        if (sourceRetentionEndTime != null)
            params.addDataParam(SOURCE_RETENTION_END_TIME, sourceRetentionEndTime);
        if (context.getTargetMtime() != null) params.addDataParam(TARGET_MTIME, context.getTargetMtime());
        if (targetMd5 != null) params.addDataParam(TARGET_MD5, targetMd5);
        if (context.getTargetRetentionEndTime() != null)
            params.addDataParam(TARGET_RETENTION_END_TIME, context.getTargetRetentionEndTime());
        if (error != null) params.addDataParam(FIRST_ERROR_MESSAGE, error);

        return params;
    }

    @Override
    public DbParams updateStatusParams(ObjectContext context, String error) {
        DbParams params = super.updateStatusParams(context, error);
        // special handling of first_error_message - this should never be overwritten if it is set
        params.removeDataParam(FIRST_ERROR_MESSAGE);
        if (error != null)
            params.addDataParam(FIRST_ERROR_MESSAGE,
                    String.format("COALESCE(%s,'%s')", FIRST_ERROR_MESSAGE, error.replace("'", "''")),
                    true);
        return params;
    }

    /**
     * Uses best-effort to populate fields based on the available columns in the result set.  If a field
     * is not present in the result set, the field is left null or whatever its default value is.
     */
    private class Mapper implements RowMapper<ExtendedSyncRecord> {
        @Override
        public ExtendedSyncRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            ExtendedSyncRecord record = new ExtendedSyncRecord();

            mapFields(rs, record);
            mapExtendedFields(rs, record);

            return record;
        }
    }

    void mapExtendedFields(ResultSet rs, ExtendedSyncRecord record) throws SQLException {
        if (hasStringColumn(rs, SOURCE_MD5.name()))
            record.setSourceMd5(rs.getString(SOURCE_MD5.name()));
        if (hasDateColumn(rs, SOURCE_RETENTION_END_TIME.name()))
            record.setSourceRetentionEndTime(dateMapper.getResultDate(rs, SOURCE_RETENTION_END_TIME.name()));
        if (hasDateColumn(rs, TARGET_MTIME.name()))
            record.setTargetMtime(dateMapper.getResultDate(rs, TARGET_MTIME.name()));
        if (hasStringColumn(rs, TARGET_MD5.name()))
            record.setTargetMd5(rs.getString(TARGET_MD5.name()));
        if (hasDateColumn(rs, TARGET_RETENTION_END_TIME.name()))
            record.setTargetRetentionEndTime(dateMapper.getResultDate(rs, TARGET_RETENTION_END_TIME.name()));
        if (hasStringColumn(rs, FIRST_ERROR_MESSAGE.name()))
            record.setFirstErrorMessage(rs.getString(FIRST_ERROR_MESSAGE.name()));
    }
}
