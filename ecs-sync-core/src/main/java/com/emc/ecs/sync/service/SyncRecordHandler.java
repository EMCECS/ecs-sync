package com.emc.ecs.sync.service;

import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

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
public class SyncRecordHandler {
    private static final Logger log = LoggerFactory.getLogger(SyncRecordHandler.class);

    public static final DbField SOURCE_ID = DbField.create("source_id", DbField.Type.string, 750, false);
    public static final DbField TARGET_ID = DbField.create("target_id", DbField.Type.string, 750, false);
    public static final DbField IS_DIRECTORY = DbField.create("is_directory", DbField.Type.bool, -1, false);
    public static final DbField SIZE = DbField.create("size", DbField.Type.bigIntNumber, -1, true);
    public static final DbField MTIME = DbField.create("mtime", DbField.Type.datetime, -1, true);
    public static final DbField STATUS = DbField.create("status", DbField.Type.string, -1, false);
    public static final DbField TRANSFER_START = DbField.create("transfer_start", DbField.Type.datetime, -1, true);
    public static final DbField TRANSFER_COMPLETE = DbField.create("transfer_complete", DbField.Type.datetime, -1, true);
    public static final DbField VERIFY_START = DbField.create("verify_start", DbField.Type.datetime, -1, true);
    public static final DbField VERIFY_COMPLETE = DbField.create("verify_complete", DbField.Type.datetime, -1, true);
    public static final DbField RETRY_COUNT = DbField.create("retry_count", DbField.Type.intNumber, -1, true);
    public static final DbField ERROR_MESSAGE = DbField.create("error_message", DbField.Type.string, -1, true);
    public static final DbField IS_SOURCE_DELETED = DbField.create("is_source_deleted", DbField.Type.bool, -1, true);

    public static final List<DbField> ALL_FIELDS = Collections.unmodifiableList(Arrays.asList(
            SOURCE_ID, TARGET_ID, IS_DIRECTORY, SIZE, MTIME, STATUS, TRANSFER_START, TRANSFER_COMPLETE,
            VERIFY_START, VERIFY_COMPLETE, RETRY_COUNT, ERROR_MESSAGE, IS_SOURCE_DELETED
    ));

    protected final int maxErrorSize;
    protected final SqlDateMapper dateMapper;

    public SyncRecordHandler(int maxErrorSize, SqlDateMapper dateMapper) {
        this.maxErrorSize = maxErrorSize;
        this.dateMapper = dateMapper;
    }

    protected List<DbField> allFields() {
        return ALL_FIELDS;
    }

    @SuppressWarnings("unchecked")
    public <T extends SyncRecord> RowMapper<T> mapper() {
        return (RowMapper<T>) new Mapper();
    }

    public String selectBySourceId(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(allFields())
                + " from " + tableName + " where " + SOURCE_ID + " = ?";
    }

    public String selectAll(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(allFields())
                + " from " + tableName;
    }

    public String selectErrors(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(allFields())
                + " from " + tableName + " where status = '" + ObjectStatus.Error.getValue() + "'";
    }

    public String selectRetries(String tableName) {
        return "select " + StringUtils.collectionToCommaDelimitedString(allFields())
                + " from " + tableName + " where status = '" + ObjectStatus.RetryQueue.getValue() + "'";
    }

    /**
     * Collects all of the parameters that should be inserted into the table, for a status update,
     * given the provide ObjectContext and error string.
     */
    public DbParams insertStatusParams(final ObjectContext context, final String error) {
        ObjectStatus status = context.getStatus();
        DbField dateField = getDateFieldForStatus(context.getStatus());
        Date dateValue = getDateValueForStatus(context.getStatus());
        boolean directory = false;
        Long contentLength = null;
        Date mtime = null;
        try {
            directory = context.getObject().getMetadata().isDirectory();
            contentLength = context.getObject().getMetadata().getContentLength();
            mtime = context.getObject().getMetadata().getModificationTime();
        } catch (Throwable t) {
            log.info("could not pull metadata from object {}: {}", context.getSourceSummary().getIdentifier(), t.toString());
        }

        DbParams params = DbParams.create();
        params.addDataParam(SOURCE_ID, context.getSourceSummary().getIdentifier());
        params.addDataParam(TARGET_ID, context.getTargetId());
        params.addDataParam(IS_DIRECTORY, directory);
        params.addDataParam(SIZE, contentLength);
        params.addDataParam(MTIME, dateMapper.getDateParam(mtime));
        params.addDataParam(STATUS, status.getValue());
        // only set a date if this is a date-tracked status
        if (dateField != null) params.addDataParam(dateField, dateMapper.getDateParam(dateValue));
        params.addDataParam(RETRY_COUNT, context.getFailures());
        params.addDataParam(ERROR_MESSAGE, fitString(error, maxErrorSize));
        return params;
    }

    /**
     * Collects all of the parameters that should be updated in the table, for a status update to
     * an existing row, given the provide ObjectContext and error string.
     */
    public DbParams updateStatusParams(final ObjectContext context, final String error) {
        DbParams params = insertStatusParams(context, error);
        // source_id will be in the where clause instead
        params.removeDataParam(SOURCE_ID);
        // don't want to overwrite last error message unless there is a new error message
        if (error == null) params.removeDataParam(ERROR_MESSAGE);
        params.addWhereClauseParam(SOURCE_ID, context.getSourceSummary().getIdentifier());
        return params;
    }

    /**
     * Collects all of the parameters that should be inserted into the table after a source object is deleted,
     * given the provide ObjectContext.
     */
    public DbParams insertDeletedParams(final ObjectContext context) {
        boolean directory = false;
        Long contentLength = null;
        Date mtime = null;
        try {
            directory = context.getObject().getMetadata().isDirectory();
            contentLength = context.getObject().getMetadata().getContentLength();
            mtime = context.getObject().getMetadata().getModificationTime();
        } catch (Throwable t) {
            log.info("could not pull metadata from object {}: {}", context.getSourceSummary().getIdentifier(), t.toString());
        }

        DbParams params = DbParams.create();
        params.addDataParam(SOURCE_ID, context.getSourceSummary().getIdentifier());
        params.addDataParam(TARGET_ID, context.getTargetId());
        params.addDataParam(IS_DIRECTORY, directory);
        params.addDataParam(SIZE, contentLength);
        params.addDataParam(MTIME, dateMapper.getDateParam(mtime));
        return params;
    }

    /**
     * Collects all of the parameters that should be updated in the table for an existing row,
     * after a source object is deleted, given the provide ObjectContext.
     */
    public DbParams updateDeletedParams(final ObjectContext context) {
        DbParams params = DbParams.create();
        params.addDataParam(IS_SOURCE_DELETED, true);
        params.addWhereClauseParam(SOURCE_ID, context.getSourceSummary().getIdentifier());
        return params;
    }

    /**
     * passing no fields will insert all fields
     */
    public static String insert(String tableName, DbParams params) {
        StringBuilder insert = new StringBuilder("insert into " + tableName + " (");

        List<DbField> insertFields = new ArrayList<>(ALL_FIELDS);

        if (params != null && params.toDataFieldList().size() > 0)
            insertFields = params.toDataFieldList();

        insert.append(StringUtils.collectionToCommaDelimitedString(insertFields));
        insert.append(") values (");
        for (int i = 0; i < insertFields.size(); i++) {
            insert.append("?");
            if (i < insertFields.size() - 1) insert.append(", ");
        }
        insert.append(")");
        return insert.toString();
    }

    /**
     * passing no fields will update all fields except source_id
     */
    public static String updateBySourceId(String tableName, DbParams params) {
        StringBuilder update = new StringBuilder("update " + tableName + " set ");

        List<DbField> updateFields = new ArrayList<>(ALL_FIELDS);
        updateFields.remove(SOURCE_ID);

        if (params != null && params.toDataFieldList().size() > 0)
            updateFields = params.toDataFieldList();

        for (int i = 0; i < updateFields.size(); i++) {
            // support for raw references in param values (will output as raw SQL)
            if (params != null && params.dataParams().get(i).rawReference()) {
                update.append(updateFields.get(i)).append("=").append(params.dataParams().get(i).value());
            } else {
                update.append(updateFields.get(i)).append("=?");
            }
            if (i < updateFields.size() - 1) update.append(", ");
        }
        update.append(" where ").append(SOURCE_ID).append(" = ?");
        return update.toString();
    }

    protected DbField getDateFieldForStatus(ObjectStatus status) {
        if (status == ObjectStatus.InTransfer) return TRANSFER_START;
        else if (status == ObjectStatus.Transferred) return TRANSFER_COMPLETE;
        else if (status == ObjectStatus.InVerification) return VERIFY_START;
        else return VERIFY_COMPLETE;
    }

    protected Date getDateValueForStatus(ObjectStatus status) {
        if (Arrays.asList(ObjectStatus.InTransfer, ObjectStatus.Transferred, ObjectStatus.InVerification, ObjectStatus.Verified)
                .contains(status))
            return new Date();
        else return null;
    }

    protected String fitString(String string, int size) {
        if (string == null) return null;
        if (string.length() > size) {
            return string.substring(0, size);
        }
        return string;
    }

    protected boolean hasColumn(ResultSet rs, String name) {
        try {
            rs.findColumn(name);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    protected boolean hasStringColumn(ResultSet rs, String name) throws SQLException {
        if (hasColumn(rs, name)) {
            String value = rs.getString(name);
            return !rs.wasNull() && value != null;
        }
        return false;
    }

    protected boolean hasLongColumn(ResultSet rs, String name) throws SQLException {
        if (hasColumn(rs, name)) {
            rs.getLong(name);
            return !rs.wasNull();
        }
        return false;
    }

    protected boolean hasBooleanColumn(ResultSet rs, String name) throws SQLException {
        if (hasColumn(rs, name)) {
            rs.getBoolean(name);
            return !rs.wasNull();
        }
        return false;
    }

    protected boolean hasDateColumn(ResultSet rs, String name) throws SQLException {
        if (hasColumn(rs, name)) {
            rs.getDate(name);
            return !rs.wasNull();
        }
        return false;
    }

    /**
     * Uses best-effort to populate fields based on the available columns in the result set.  If a field
     * is not present in the result set, the field is left null or whatever its default value is.
     */
    private class Mapper implements RowMapper<SyncRecord> {
        @Override
        public SyncRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            SyncRecord record = new SyncRecord();

            mapFields(rs, record);

            return record;
        }
    }

    void mapFields(ResultSet rs, SyncRecord record) throws SQLException {
        if (!hasColumn(rs, SOURCE_ID.name()))
            throw new IllegalArgumentException("result set does not have a column named " + SOURCE_ID);
        record.setSourceId(rs.getString(SOURCE_ID.name()));

        if (hasStringColumn(rs, TARGET_ID.name())) record.setTargetId(rs.getString(TARGET_ID.name()));
        if (hasBooleanColumn(rs, IS_DIRECTORY.name()))
            record.setDirectory(rs.getBoolean(IS_DIRECTORY.name()));
        if (hasLongColumn(rs, SIZE.name())) record.setSize(rs.getLong(SIZE.name()));
        if (hasDateColumn(rs, MTIME.name())) record.setMtime(dateMapper.getResultDate(rs, MTIME.name()));
        if (hasStringColumn(rs, STATUS.name()))
            record.setStatus(ObjectStatus.fromValue(rs.getString(STATUS.name())));
        if (hasDateColumn(rs, TRANSFER_START.name()))
            record.setTransferStart(dateMapper.getResultDate(rs, TRANSFER_START.name()));
        if (hasDateColumn(rs, TRANSFER_COMPLETE.name()))
            record.setTransferComplete(dateMapper.getResultDate(rs, TRANSFER_COMPLETE.name()));
        if (hasDateColumn(rs, VERIFY_START.name()))
            record.setVerifyStart(dateMapper.getResultDate(rs, VERIFY_START.name()));
        if (hasDateColumn(rs, VERIFY_COMPLETE.name()))
            record.setVerifyComplete(dateMapper.getResultDate(rs, VERIFY_COMPLETE.name()));
        if (hasLongColumn(rs, RETRY_COUNT.name())) record.setRetryCount(rs.getInt(RETRY_COUNT.name()));
        if (hasStringColumn(rs, ERROR_MESSAGE.name()))
            record.setErrorMessage(rs.getString(ERROR_MESSAGE.name()));
        if (hasBooleanColumn(rs, IS_SOURCE_DELETED.name()))
            record.setSourceDeleted(rs.getBoolean(IS_SOURCE_DELETED.name()));
    }
}
