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

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.util.Function;
import com.emc.ecs.sync.util.TimingUtil;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractDbService implements DbService, SqlDateMapper {
    public static final String OPERATION_OBJECT_QUERY = "ObjectQuery";
    public static final String OPERATION_OBJECT_UPDATE = "ObjectUpdate";

    public static final String DEFAULT_OBJECTS_TABLE_NAME = "objects";
    public static final int DEFAULT_MAX_ERROR_SIZE = 2048;

    protected String objectsTableName = DEFAULT_OBJECTS_TABLE_NAME;
    protected int maxErrorSize = DEFAULT_MAX_ERROR_SIZE;
    protected final boolean extendedFieldsEnabled;
    private JdbcTemplate jdbcTemplate;
    private volatile boolean initialized = false;
    private final Set<String> locks = new HashSet<>();
    private final SyncRecordHandler recordHandler;

    protected abstract JdbcTemplate createJdbcTemplate();

    protected abstract void createTable();

    public AbstractDbService(boolean extendedFieldsEnabled) {
        this.extendedFieldsEnabled = extendedFieldsEnabled;
        if (extendedFieldsEnabled) {
            recordHandler = new ExtendedSyncRecordHandler(maxErrorSize, this);
        } else {
            recordHandler = new SyncRecordHandler(maxErrorSize, this);
        }
    }

    /**
     * Be sure we close resources before GC
     */
    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    @Override
    public void lock(String identifier) {
        synchronized (locks) {
            while (locks.contains(identifier)) {
                try {
                    locks.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException("interrupted while waiting for lock", e);
                }
            }
            locks.add(identifier);
        }
    }

    @Override
    public void unlock(String identifier) {
        synchronized (locks) {
            locks.remove(identifier);
            locks.notifyAll();
        }
    }

    @Override
    public boolean setStatus(final ObjectContext context, final String error, final boolean newRow) {
        DbParams params = newRow
                ? recordHandler.insertStatusParams(context, error)
                : recordHandler.updateStatusParams(context, error);
        executeUpdate(params, newRow, context.getOptions());
        return true;
    }

    @Override
    public boolean setDeleted(final ObjectContext context, final boolean newRow) {
        DbParams params = newRow
                ? recordHandler.insertDeletedParams(context)
                : recordHandler.updateDeletedParams(context);
        executeUpdate(params, newRow, context.getOptions());
        return true;
    }

    protected void executeUpdate(final DbParams params, final boolean newRow, final SyncOptions syncOptions) {
        initCheck();

        TimingUtil.time(syncOptions, OPERATION_OBJECT_UPDATE, () -> {
            if (newRow) {
                String insertSql = SyncRecordHandler.insert(objectsTableName, params);
                getJdbcTemplate().update(insertSql, params.toParamValueArray());
            } else {
                String insertSql = SyncRecordHandler.updateBySourceId(objectsTableName, params);
                getJdbcTemplate().update(insertSql, params.toParamValueArray());
            }
            return null;
        });
    }

    @Override
    public SyncRecord getSyncRecord(final ObjectContext context) {
        initCheck();
        return TimingUtil.time(context.getOptions(), OPERATION_OBJECT_QUERY, (Function<SyncRecord>) () -> {
            try {
                return getJdbcTemplate().queryForObject(recordHandler.selectBySourceId(objectsTableName),
                        recordHandler.mapper(), context.getSourceSummary().getIdentifier());
            } catch (IncorrectResultSizeDataAccessException e) {
                return null;
            }
        });
    }

    public <T extends SyncRecord> Iterable<T> getAllRecords() {
        initCheck();
        return () -> new RowIterator<>(
                getJdbcTemplate().getDataSource(),
                recordHandler.mapper(),
                recordHandler.selectAll(objectsTableName));
    }

    @Override
    public <T extends SyncRecord> Iterable<T> getSyncErrors() {
        initCheck();
        return () -> new RowIterator<>(
                getJdbcTemplate().getDataSource(),
                recordHandler.mapper(),
                recordHandler.selectErrors(objectsTableName));
    }

    @Override
    public <T extends SyncRecord> Iterable<T> getSyncRetries() {
        initCheck();
        return () -> new RowIterator<>(
                getJdbcTemplate().getDataSource(),
                recordHandler.mapper(),
                recordHandler.selectRetries(objectsTableName));
    }

    protected void initCheck() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    jdbcTemplate = createJdbcTemplate();
                    createTable();
                    initialized = true;
                }
            }
        }
    }

    /**
     * Be sure to override in implementations to close the datasource completely, then call super.close(). This method
     * should be idempotent! (it might get called twice)
     */
    @Override
    public void close() {
        jdbcTemplate = null;
    }

    protected JdbcTemplate getJdbcTemplate() {
        if (jdbcTemplate == null)
            throw new UnsupportedOperationException("this service is not initialized or has been closed");
        return jdbcTemplate;
    }

    public Date getResultDate(ResultSet rs, String name) throws SQLException {
        return rs.getDate(name);
    }

    public Object getDateParam(Date date) {
        return date;
    }

    @Override
    public String getObjectsTableName() {
        return objectsTableName;
    }

    @Override
    public void setObjectsTableName(String objectsTableName) {
        this.objectsTableName = objectsTableName;
    }

    @Override
    public int getMaxErrorSize() {
        return maxErrorSize;
    }

    @Override
    public void setMaxErrorSize(int maxErrorSize) {
        this.maxErrorSize = maxErrorSize;
    }

    @Override
    public boolean isExtendedFieldsEnabled() {
        return extendedFieldsEnabled;
    }
}
