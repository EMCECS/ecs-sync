/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.target;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Inserts objects into a SQL database as BLOBs.
 *
 * @author cwikj
 */
public class SqlBlobTarget extends SyncTarget {
    private DataSource dataSource;
    private String insertSql;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return false; // this plug-in is not CLI capable
    }

    @Override
    public Options getCustomOptions() {
        return new Options();
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
    }

    @Override
    public void validateChain(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
    }

    @Override
    public void filter(SyncObject obj) {
        if (!obj.hasData()) {
            return;
        }
        try (Connection con = dataSource.getConnection();
             PreparedStatement ps = con.prepareStatement(insertSql);
             InputStream in = obj.getInputStream()) {
            ps.setBinaryStream(1, in, obj.getSize());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to insert into database: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to stream object data: " + e.getMessage(), e);
        }
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDocumentation() {
        return null;
    }

    /**
     * @return the dataSource
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * @param dataSource the dataSource to set
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * @return the insertSql
     */
    public String getInsertSql() {
        return insertSql;
    }

    /**
     * @param insertSql the insertSql to set
     */
    public void setInsertSql(String insertSql) {
        this.insertSql = insertSql;
    }
}
