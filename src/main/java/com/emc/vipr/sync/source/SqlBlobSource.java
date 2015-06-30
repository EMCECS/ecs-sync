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
package com.emc.vipr.sync.source;

import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.object.AbstractSyncObject;
import com.emc.vipr.sync.model.object.S3SyncObject;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.target.AtmosTarget;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.ReadOnlyIterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Iterator;
import java.util.Map;

/**
 * Implements a source plugin that reads BLOB objects from a SQL table, migrates
 * them to Atmos, and then updates another (possibly same) table with the new
 * Atmos identifier. Due to this source's complexity, it may be easiest to
 * configure this source using Spring. See spring/sqlsource.xml for a sample.
 * <hr>
 * Source Properties:
 * <dl>
 * <dt>dataSource</dt>
 * <dd>The javax.sql.DataSource to use for connections</dd>
 * <dt>selectSql</dt>
 * <dd>The query to execute to select objects to copy to Atmos</dd>
 * <dt>sourceBlobColumn</dt>
 * <dd>The column within the sourceQuery that contains the BLOB data</dd>
 * <dt>sourceIdColumn</dt>
 * <dd>The column within the sourceQuery that contains the object identifier</dd>
 * <dt>targetIdColumn</dt>
 * <dd>(Optional) If specified, the target identifier will be fetched from this
 * column. If non-empty, this will be used as the target Object identifier and
 * the object will be updated inside the target instead of creating a new object.
 * Note that if this property is omitted, every object will be a new create
 * operation even over multiple runs.</dd>
 * <dt>metadataMapping</dt>
 * <dd>(Optional) If specified, the specified columns will be read from the
 * select data and used as Atmos metadata.</dd>
 * <dt>metadataTrim</dt>
 * <dd>(Default=true) If true, any metadata read from the source will be
 * trimmed. This is useful when data is read from databases with fixed-length
 * CHAR columns to remove extra spaces</dd>
 * <dt>updateSql</dt>
 * <dd>(Optional) The query to execute after storing data in Atmos. this is used
 * to store the Atmos ID in the database. This could also set the BLOB column to
 * null to release the data.</dd>
 * <dt>updateIdColumn</dt>
 * <dd>This is the column to set with the value read from sourceIdColumn during
 * the select operation</dd>
 * <dt>updateAtmosIdColumn</dt>
 * <dd>This is the column to set the new Atmos ID in</dd>
 * </dl>
 *
 * @author cwikj
 */
public class SqlBlobSource extends SyncSource<SqlBlobSource.SqlSyncObject> {
    private static final Logger l4j = Logger.getLogger(SqlBlobSource.class);

    private DataSource dataSource;
    private String selectSql;
    private String sourceBlobColumn;
    private String sourceIdColumn;
    private String targetIdColumn;
    private Map<String, String> metadataMapping;
    private boolean metadataTrim = true;
    private String updateSql;
    private int updateIdColumn;
    private int updateTargetIdColumn;

    /**
     * This plugin does not support CLI configuration.
     */
    @Override
    public boolean canHandleSource(String sourceUri) {
        return false;
    }

    @Override
    public Options getCustomOptions() {
        return new Options();
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (!(target instanceof AtmosTarget)) {
            throw new ConfigurationException("Target plugin must be AtmosTarget");
        }

        if (((AtmosTarget) target).getDestNamespace() != null) {
            throw new ConfigurationException("Target plugin must be in Object mode not Namespace");
        }
    }

    @Override
    public Iterator<SqlSyncObject> iterator() {
        return new ReadOnlyIterator<SqlSyncObject>() {
            Connection con;
            PreparedStatement ps;
            ResultSet rs;

            @Override
            protected SqlSyncObject getNextObject() {
                try {
                    if (rs == null) {
                        con = dataSource.getConnection();
                        ps = con.prepareStatement(selectSql);
                        rs = ps.executeQuery();
                    }

                    if (rs.next()) {
                        SqlSyncObject sso = new SqlSyncObject(SqlBlobSource.this, rs.getObject(sourceIdColumn), rs.getBlob(sourceBlobColumn));

                        // Are we tracking target IDs?
                        if (targetIdColumn != null) {
                            String soid = rs.getString(targetIdColumn);
                            if (soid != null && soid.trim().length() > 0) {
                                sso.setTargetIdentifier(soid.trim());
                            }
                        }

                        // Metadata
                        if (metadataMapping != null) {
                            for (Map.Entry<String, String> entry : metadataMapping.entrySet()) {
                                String metaName = entry.getValue();
                                String value = rs.getString(entry.getKey());
                                if (value == null) continue;
                                if (metadataTrim) value = value.trim();
                                sso.getMetadata().setUserMetadataValue(metaName, value);
                            }
                        }

                        return sso;
                    } else {
                        l4j.info("Reached end of query");
                        try {
                            rs.close();
                            ps.close();
                            con.close();
                        } catch (SQLException e) {
                            l4j.warn("Error closing resources: " + e, e);
                        }
                        return null;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("error in select query", e);
                }
            }
        };
    }

    @Override
    public void sync(SqlSyncObject syncObject, SyncFilter filterChain) {
        filterChain.filter(syncObject);

        // update DB with new object ID
        if (updateSql != null) {
            Object sqlId = syncObject.getRawSourceIdentifier();
            String objectId = syncObject.getTargetIdentifier();

            // Run the update SQL
            Connection con = null;
            PreparedStatement ps = null;

            try {

                con = dataSource.getConnection();
                ps = con.prepareStatement(updateSql);
                ps.setObject(updateIdColumn, sqlId);
                ps.setString(updateTargetIdColumn, objectId);

                ps.executeUpdate();
            } catch (Exception e) {
                l4j.error(String.format("SQL update failed [%s -> %s]", sqlId, objectId), e);
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        // Ignore
                    }
                }
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        // Ignore
                    }
                }
            }
        }
    }

    @Override
    public Iterator<SqlSyncObject> childIterator(SqlSyncObject syncObject) {
        return null;
    }

//    @Override
//    public void delete(S3SyncObject syncObject) {
//
//    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDocumentation() {
        return null;
    }

    /**
     * SyncObject subclass for handling SQL blobs.
     */
    protected static class SqlSyncObject extends AbstractSyncObject<Object> {
        private SyncPlugin parentPlugin;
        private Blob blob;

        public SqlSyncObject(SyncPlugin parentPlugin, Object sqlId, Blob blob) {
            super(sqlId, sqlId.toString(), sqlId.toString(), false);
            this.parentPlugin = parentPlugin;
            this.blob = blob;
        }

        @Override
        protected InputStream createSourceInputStream() {
            try {
                return new BufferedInputStream(blob.getBinaryStream(), parentPlugin.getBufferSize());
            } catch (SQLException e) {
                throw new RuntimeException("failed to get blob stream", e);
            }
        }

        @Override
        protected void loadObject() {
            metadata = new SyncMetadata();
            try {
                metadata.setSize(blob.length());
            } catch (SQLException e) {
                throw new RuntimeException("error getting blob length", e);
            }
        }
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
     * @return the selectSql
     */
    public String getSelectSql() {
        return selectSql;
    }

    /**
     * @param selectSql the selectSql to set
     */
    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }

    /**
     * @return the sourceBlobColumn
     */
    public String getSourceBlobColumn() {
        return sourceBlobColumn;
    }

    /**
     * @param sourceBlobColumn the sourceBlobColumn to set
     */
    public void setSourceBlobColumn(String sourceBlobColumn) {
        this.sourceBlobColumn = sourceBlobColumn;
    }

    /**
     * @return the sourceIdColumn
     */
    public String getSourceIdColumn() {
        return sourceIdColumn;
    }

    /**
     * @param sourceIdColumn the sourceIdColumn to set
     */
    public void setSourceIdColumn(String sourceIdColumn) {
        this.sourceIdColumn = sourceIdColumn;
    }

    public String getTargetIdColumn() {
        return targetIdColumn;
    }

    public void setTargetIdColumn(String targetIdColumn) {
        this.targetIdColumn = targetIdColumn;
    }

    /**
     * @return the metadataMapping
     */
    public Map<String, String> getMetadataMapping() {
        return metadataMapping;
    }

    /**
     * @param metadataMapping the metadataMapping to set
     */
    public void setMetadataMapping(Map<String, String> metadataMapping) {
        this.metadataMapping = metadataMapping;
    }

    /**
     * @return the metadataTrim
     */
    public boolean isMetadataTrim() {
        return metadataTrim;
    }

    /**
     * @param metadataTrim the metadataTrim to set
     */
    public void setMetadataTrim(boolean metadataTrim) {
        this.metadataTrim = metadataTrim;
    }

    /**
     * @return the updateSql
     */
    public String getUpdateSql() {
        return updateSql;
    }

    /**
     * @param updateSql the updateSql to set
     */
    public void setUpdateSql(String updateSql) {
        this.updateSql = updateSql;
    }

    /**
     * @return the updateIdColumn
     */
    public int getUpdateIdColumn() {
        return updateIdColumn;
    }

    /**
     * @param updateIdColumn the updateIdColumn to set
     */
    public void setUpdateIdColumn(int updateIdColumn) {
        this.updateIdColumn = updateIdColumn;
    }

    public int getUpdateTargetIdColumn() {
        return updateTargetIdColumn;
    }

    public void setUpdateTargetIdColumn(int updateTargetIdColumn) {
        this.updateTargetIdColumn = updateTargetIdColumn;
    }
}
