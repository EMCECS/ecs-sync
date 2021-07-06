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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class SqliteDbService extends AbstractDbService {
    private static final Logger log = LoggerFactory.getLogger(SqliteDbService.class);

    public static final String JDBC_URL_BASE = "jdbc:sqlite:";

    private File dbFile;
    private final String jdbcUrl;
    private volatile boolean closed;

    public SqliteDbService(File dbFile, boolean extendedFieldsEnabled) {
        this(JDBC_URL_BASE + dbFile.toString(), extendedFieldsEnabled);
        this.dbFile = dbFile;
        if ((!dbFile.exists() && dbFile.getParentFile() != null && !dbFile.getParentFile().canWrite())
                || (dbFile.exists() && !dbFile.canWrite()))
            throw new IllegalArgumentException("Cannot write to " + dbFile);
    }

    protected SqliteDbService(String jdbcUrl, boolean extendedFieldsEnabled) {
        super(extendedFieldsEnabled);
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    public void deleteDatabase() {
        if (dbFile != null && !dbFile.delete())
            log.warn("could not delete database file {}", dbFile);
    }

    @Override
    public void close() {
        try {
            if (!closed) ((SingleConnectionDataSource) getJdbcTemplate().getDataSource()).destroy();
        } catch (Throwable t) {
            log.warn("could not close data source", t);
        }
        closed = true;
        super.close();
    }

    @Override
    protected JdbcTemplate createJdbcTemplate() {
        SingleConnectionDataSource ds = new SingleConnectionDataSource();
        ds.setUrl(jdbcUrl);
        ds.setSuppressClose(true);
        return new JdbcTemplate(ds);
    }

    @Override
    protected void createTable() {
        try {
            if (extendedFieldsEnabled) {
                // TODO: support altering existing tables to add extended fields
                getJdbcTemplate().update("CREATE TABLE IF NOT EXISTS " + getObjectsTableName() + " (" +
                        "source_id VARCHAR(1500) PRIMARY KEY NOT NULL," +
                        "target_id VARCHAR(1500)," +
                        "is_directory INT NOT NULL," +
                        "size INT," +
                        "mtime INT," +
                        "status VARCHAR(32) NOT NULL," +
                        "transfer_start INT," +
                        "transfer_complete INT," +
                        "verify_start INT," +
                        "verify_complete INT," +
                        "retry_count INT," +
                        "error_message VARCHAR(" + getMaxErrorSize() + ")," +
                        "is_source_deleted INT NULL," +
                        "source_md5 VARCHAR(32)," +
                        "source_retention_end_time INT," +
                        "target_mtime INT," +
                        "target_md5 VARCHAR(32)," +
                        "target_retention_end_time INT," +
                        "first_error_message VARCHAR(" + getMaxErrorSize() + ")" +
                        ")");
            } else {
                getJdbcTemplate().update("CREATE TABLE IF NOT EXISTS " + getObjectsTableName() + " (" +
                        "source_id VARCHAR(1500) PRIMARY KEY NOT NULL," +
                        "target_id VARCHAR(1500)," +
                        "is_directory INT NOT NULL," +
                        "size INT," +
                        "mtime INT," +
                        "status VARCHAR(32) NOT NULL," +
                        "transfer_start INT," +
                        "transfer_complete INT," +
                        "verify_start INT," +
                        "verify_complete INT," +
                        "retry_count INT," +
                        "error_message VARCHAR(" + getMaxErrorSize() + ")," +
                        "is_source_deleted INT NULL" +
                        ")");
            }
        } catch (RuntimeException e) {
            log.error("could not create DB table {}. note: name may only contain alphanumeric or underscore", getObjectsTableName());
            throw e;
        }
    }

    @Override
    public Date getResultDate(ResultSet rs, String name) throws SQLException {
        return new Date(rs.getLong(name));
    }

    @Override
    public Object getDateParam(Date date) {
        if (date == null) return null;
        return date.getTime();
    }

    public File getDbFile() {
        return dbFile;
    }
}
