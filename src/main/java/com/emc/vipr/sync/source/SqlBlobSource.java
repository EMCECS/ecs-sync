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

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SqlIdAnnotation;
import com.emc.vipr.sync.model.SyncObject;
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
 * <dt>sourceAtmosIdColumn</dt>
 * <dd>(Optional) If specified, the Atmos objectID will be fetched from this
 * column. If non-empty, this will be used as the Atmos Object identifier and
 * the object will be updated inside Atmos instead of creating a new object.
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
    private String sourceAtmosIdColumn;
    private Map<String, String> metadataMapping;
    private boolean metadataTrim = true;
    private String updateSql;
    private int updateIdColumn;
    private int updateAtmosIdColumn;

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
                        SqlSyncObject sso = new SqlSyncObject(rs.getObject(sourceIdColumn), rs.getBlob(sourceBlobColumn));

                        // Are we tracking target OIDs?
                        if (sourceAtmosIdColumn != null) {
                            String soid = rs.getString(sourceAtmosIdColumn);
                            if (soid != null && soid.trim().length() > 0) {
                                sso.setTargetIdentifier(rs.getString(sourceAtmosIdColumn));
                            }
                        }

                        // Metadata
                        if (metadataMapping != null) {
                            for (Map.Entry<String, String> entry : metadataMapping.entrySet()) {
                                String metaName = entry.getValue();
                                String value = rs.getString(entry.getKey());
                                if (value == null) continue;
                                if (metadataTrim) value = value.trim();
                                sso.getMetadata().setUserMetadataProp(metaName, value);
                            }
                        }

                        return sso;
                    } else {
                        // no more results
                        return null;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("error in select query", e);
                }
            }
        };
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDocumentation() {
        return null;
    }

    @Override
    public void onSuccess(SqlSyncObject syncObject) {
        if (updateSql != null) {
            Object sqlId = syncObject.getAnnotation(SqlIdAnnotation.class).getSqlId();
            String objectId = syncObject.getTargetIdentifier();

            // Run the update SQL
            Connection con = null;
            PreparedStatement ps = null;

            try {

                con = dataSource.getConnection();
                ps = con.prepareStatement(updateSql);
                ps.setObject(updateIdColumn, sqlId);
                ps.setString(updateAtmosIdColumn, objectId);

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

    /**
     * SyncObject subclass for handling SQL blobs.
     */
    protected class SqlSyncObject extends SyncObject<SqlSyncObject> {
        private Blob blob;

        public SqlSyncObject(Object sqlId, Blob blob) {
            super(sqlId.toString(), sqlId.toString());
            this.blob = blob;

            addAnnotation(new SqlIdAnnotation(sqlId));
        }

        @Override
        public Object getRawSourceIdentifier() {
            return sourceIdentifier;
        }

        @Override
        public boolean hasData() {
            return true;
        }

        @Override
        public long getSize() {
            try {
                return blob.length();
            } catch (SQLException e) {
                throw new RuntimeException("error getting blob length", e);
            }
        }

        @Override
        protected InputStream createSourceInputStream() {
            try {
                return new BufferedInputStream(blob.getBinaryStream(), bufferSize);
            } catch (SQLException e) {
                throw new RuntimeException("failed to get Blob stream", e);
            }
        }

        @Override
        public boolean hasChildren() {
            return false;
        }

        @Override
        public Iterator<SqlSyncObject> childIterator() {
            return null;
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

    /**
     * @return the sourceAtmosIdColumn
     */
    public String getSourceAtmosIdColumn() {
        return sourceAtmosIdColumn;
    }

    /**
     * @param sourceAtmosIdColumn the sourceAtmosIdColumn to set
     */
    public void setSourceAtmosIdColumn(String sourceAtmosIdColumn) {
        this.sourceAtmosIdColumn = sourceAtmosIdColumn;
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

    /**
     * @return the updateAtmosIdColumn
     */
    public int getUpdateAtmosIdColumn() {
        return updateAtmosIdColumn;
    }

    /**
     * @param updateAtmosIdColumn the updateAtmosIdColumn to set
     */
    public void setUpdateAtmosIdColumn(int updateAtmosIdColumn) {
        this.updateAtmosIdColumn = updateAtmosIdColumn;
    }
}
