/*
 * Copyright (c) 2015-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.util.ReadOnlyIterator;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;

import javax.sql.DataSource;
import java.io.Closeable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Iterates an SQL ResultSet, returning each row as an object mapped by the given RowMapper. Used to reduce the
 * memory footprint of large queries.
 * <p/>
 * Instances will automatically close resources when it is exhausted or if an error occurs. If you do not exhaust
 * the iterator, you *must* manually close it!
 */
public class RowIterator<T> extends ReadOnlyIterator<T> implements Closeable {
    private Connection con;
    private PreparedStatement st;
    private ResultSet rs;
    private RowMapper<T> rowMapper;
    private AtomicInteger rowNum = new AtomicInteger();

    public RowIterator(DataSource ds, RowMapper<T> rowMapper, String query, Object... params) {
        try {
            this.rowMapper = rowMapper;
            this.con = ds.getConnection();
            this.st = con.prepareStatement(query, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
            if (params != null && params.length > 0)
                new ArgumentPreparedStatementSetter(params).setValues(st);
            this.rs = st.executeQuery();
        } catch (SQLException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    @Override
    protected T getNextObject() {
        try {
            if (rs.next()) return rowMapper.mapRow(rs, rowNum.incrementAndGet());
            close(); // result set is exhausted
            return null;
        } catch (SQLException e) {
            close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        JdbcUtils.closeResultSet(rs);
        JdbcUtils.closeStatement(st);
        JdbcUtils.closeConnection(con);
    }
}
