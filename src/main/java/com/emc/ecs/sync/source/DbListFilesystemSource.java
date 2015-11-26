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
package com.emc.ecs.sync.source;

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.ReadOnlyIterator;
import com.emc.ecs.sync.model.object.FileSyncObject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.Assert;

import javax.activation.MimetypesFileTypeMap;
import javax.sql.DataSource;
import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * This is an extension of the filesystem source that reads its list of
 * files to transfer from a database SELECT query.
 * <p/>
 * Note that this source currently only supports Spring configuration.
 */
public class DbListFilesystemSource extends FilesystemSource {
    private DataSource dataSource;
    private String selectQuery;
    private List<String> metadataColumns;
    private String filenameColumn;

    /**
     * This plugin does not support CLI configuration.
     */
    @Override
    public boolean canHandleSource(String sourceUri) {
        return false;
    }

    @Override
    public Iterator<FileSyncObject> iterator() {
        return new ReadOnlyIterator<FileSyncObject>() {
            JdbcTemplate jdbc;
            SqlRowSet rs;

            @Override
            protected FileSyncObject getNextObject() {
                if (rs == null) {
                    jdbc = new JdbcTemplate(dataSource);
                    rs = jdbc.queryForRowSet(selectQuery);
                }

                if (rs.next()) {
                    File file = new File(rs.getString(filenameColumn));
                    FileSyncObject object = new FileSyncObject(DbListFilesystemSource.this, new MimetypesFileTypeMap(),
                            file, getRelativePath(file), isFollowLinks());
                    if (metadataColumns != null) {
                        for (String colName : metadataColumns) {
                            String value = rs.getString(colName);
                            if (value != null) {
                                object.getMetadata().setUserMetadataValue(colName, value);
                            }
                        }
                    }
                    return object;
                } else {
                    // no more results
                    return null;
                }
            }
        };
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.hasText(filenameColumn,
                "The property 'filenameColumn' is required");
        Assert.hasText(selectQuery, "The property 'selectQuery' is required.");
        Assert.notNull(dataSource, "The property 'dataSource' is required.");
        setUseAbsolutePath(true);
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
     * @return the selectQuery
     */
    public String getSelectQuery() {
        return selectQuery;
    }

    /**
     * @param selectQuery the selectQuery to set
     */
    public void setSelectQuery(String selectQuery) {
        this.selectQuery = selectQuery;
    }

    /**
     * @return the metadataColumns
     */
    public List<String> getMetadataColumns() {
        return metadataColumns;
    }

    /**
     * @param metadataColumns the metadataColumns to set
     */
    public void setMetadataColumns(List<String> metadataColumns) {
        this.metadataColumns = metadataColumns;
    }

    /**
     * @return the filenameColumn
     */
    public String getFilenameColumn() {
        return filenameColumn;
    }

    /**
     * @param filenameColumn the filenameColumn to set
     */
    public void setFilenameColumn(String filenameColumn) {
        this.filenameColumn = filenameColumn;
    }
}
