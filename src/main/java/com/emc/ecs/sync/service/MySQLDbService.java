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

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class MySQLDbService extends AbstractDbService {
    private static final Logger log = LoggerFactory.getLogger(MySQLDbService.class);

    private String connectString;
    private String username;
    private String password;
    private volatile boolean closed;

    public MySQLDbService(String connectString, String username, String password) {
        this.connectString = connectString;
        this.username = username;
        this.password = password;
    }

    @Override
    public void deleteDatabase() {
        JdbcTemplate template = createJdbcTemplate();
        try {
            template.execute("drop table if exists " + getObjectsTableName());
        } finally {
            close(template);
        }
    }

    @Override
    public void close() {
        try {
            if (!closed) close(getJdbcTemplate());
        } finally {
            closed = true;
            super.close();
        }
    }

    protected void close(JdbcTemplate template) {
        try {
            ((HikariDataSource) template.getDataSource()).close();
        } catch (RuntimeException e) {
            log.warn("could not close data source", e);
        }
    }

    @Override
    protected JdbcTemplate createJdbcTemplate() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(connectString);
        if (username != null) ds.setUsername(username);
        if (password != null) ds.setPassword(password);
        ds.setMaximumPoolSize(500);
        ds.setMinimumIdle(10);
        ds.addDataSourceProperty("characterEncoding", "utf8");
        ds.addDataSourceProperty("cachePrepStmts", "true");
        ds.addDataSourceProperty("prepStmtCacheSize", "250");
        ds.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        return new JdbcTemplate(ds);
    }

    @Override
    protected void createTable() {
        getJdbcTemplate().update("CREATE TABLE IF NOT EXISTS " + getObjectsTableName() + " (" +
                "source_id VARCHAR(750) PRIMARY KEY NOT NULL," +
                "target_id VARCHAR(1500)," +
                "is_directory INT NOT NULL," +
                "size BIGINT," +
                "mtime DATETIME," +
                "status VARCHAR(32) NOT NULL," +
                "transfer_start DATETIME null," +
                "transfer_complete DATETIME null," +
                "verify_start DATETIME null," +
                "verify_complete DATETIME null," +
                "retry_count INT," +
                "error_message VARCHAR(" + getMaxErrorSize() + ")," +
                "is_source_deleted INT NULL," +
                "INDEX status_idx (status)" +
                ") ENGINE=InnoDB ROW_FORMAT=COMPRESSED");
    }

    @Override
    protected Date getResultDate(ResultSet rs, String name) throws SQLException {
        return new Date(rs.getTimestamp(name).getTime());
    }

    @Override
    protected Object getDateParam(Date date) {
        if (date == null) return null;
        return new java.sql.Timestamp(date.getTime());
    }
}
