/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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

    private String dbFile;
    private volatile boolean closed;

    public SqliteDbService(String dbFile) {
        this.dbFile = dbFile;
        if (!dbFile.startsWith(":")) { // don't validate non-file locations (like :memory:)
            File file = new File(dbFile);
            if ((!file.exists() && file.getParentFile() != null && !file.getParentFile().canWrite())
                    || (file.exists() && !file.canWrite()))
                throw new IllegalArgumentException("Cannot write to " + dbFile);
        }
    }

    @Override
    public void deleteDatabase() {
        if (!dbFile.contains(":") && !new File(dbFile).delete())
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
        ds.setUrl(JDBC_URL_BASE + getDbFile());
        ds.setSuppressClose(true);
        return new JdbcTemplate(ds);
    }

    @Override
    protected void createTable() {
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

    @Override
    protected Date getResultDate(ResultSet rs, String name) throws SQLException {
        return new Date(rs.getLong(name));
    }

    @Override
    protected Object getDateParam(Date date) {
        if (date == null) return null;
        return date.getTime();
    }

    public String getDbFile() {
        return dbFile;
    }
}
