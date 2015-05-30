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
package com.emc.vipr.sync.filter;

import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.Function;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.SyncUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;

/**
 * Tracks sync status in a database with source/target IDs and successes/failures
 * with timestamps.
 */
public class TrackingFilter extends SyncFilter {
    private static final Logger l4j = Logger.getLogger(TrackingFilter.class);

    public static final String ACTIVATION_NAME = "tracking";

    private static final String STATUS_TABLE = "sync_status";
    private static final String SQL_FIND_TABLE = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = ?";
    private static final String SQL_STATUS_QUERY = "select * from %s where source_id = ?";

    public static final String IN_PROCESS_STATUS = "In Process";
    public static final String COMPLETE_STATUS = "Complete";
    public static final String ERROR_STATUS = "Error";
    public static final String VERIFIED_STATUS = "Verified";
    public static final List<String> SKIP_STATUSES = Arrays.asList(COMPLETE_STATUS, VERIFIED_STATUS);

    public static final String DB_URL_OPT = "tracking-db-url";
    public static final String DB_URL_DESC = "The JDBC URL to the database";
    public static final String DB_URL_ARG_NAME = "jdbc-url";

    public static final String DB_DRIVER_OPT = "tracking-db-driver";
    public static final String DB_DRIVER_DESC = "The JDBC database driver class (if using an older driver)";
    public static final String DB_DRIVER_ARG_NAME = "driver-class";

    public static final String DB_USER_OPT = "tracking-db-user";
    public static final String DB_USER_DESC = "The database userid";
    public static final String DB_USER_ARG_NAME = "userid";

    public static final String DB_PASSWORD_OPT = "tracking-db-password";
    public static final String DB_PASSWORD_DESC = "The database password";
    public static final String DB_PASSWORD_ARG_NAME = "password";

    public static final String TABLE_OPT = "tracking-table";
    public static final String TABLE_DESC = "Specify the name of the tracking table if it differs from the default (" + STATUS_TABLE + ")";
    public static final String TABLE_ARG_NAME = "table-name";

    public static final String CREATE_TABLE_OPT = "tracking-create-table";
    public static final String CREATE_TABLE_DESC = "Use this option to automatically create the tracking table in the database if it does not already exist.";

    public static final String REPROCESS_OPT = "reprocess-completed";
    public static final String REPROCESS_DESC = "Normally if an object is shown as Complete in the tracking table, it is skipped (not sent to the target for transfer). This option will send all objects to the target, which may or may not have its own logic to minimize redundant transfers.";

    public static final String META_OPT = "tracking-metadata";
    public static final String META_DESC = "A comma-separated list of metadata names whose values should be added to the tracking table, e.g. \"size,mtime,ctime\". If specified, each value will be pulled from the source object and added to the tracking insert/update statement using metadata name as the column name. Can be used in combination with --" + CREATE_TABLE_OPT + " so long as the metadata names do not change from run to run (does not update the table DDL once created).";
    public static final String META_ARG_NAME = "tag-list";

    public static final int MESSAGE_COLUMN_SIZE = 2048;

    // timed operations
    private static final String OPERATION_STATUS_QUERY = "StatusQuery";
    private static final String OPERATION_STATUS_UPDATE = "StatusUpdate";

    private DataSource dataSource;
    private String tableName = STATUS_TABLE;
    private boolean createTable = false;
    private boolean processAllObjects = false;
    private List<String> metaTags = new ArrayList<String>();

    private JdbcTemplate template;

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(DB_URL_OPT).withDescription(DB_URL_DESC)
                .hasArg().withArgName(DB_URL_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DB_DRIVER_OPT).withDescription(DB_DRIVER_DESC)
                .hasArg().withArgName(DB_DRIVER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DB_USER_OPT).withDescription(DB_USER_DESC)
                .hasArg().withArgName(DB_USER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DB_PASSWORD_OPT).withDescription(DB_PASSWORD_DESC)
                .hasArg().withArgName(DB_PASSWORD_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(TABLE_OPT).withDescription(TABLE_DESC)
                .hasArg().withArgName(TABLE_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(CREATE_TABLE_OPT).withDescription(CREATE_TABLE_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(REPROCESS_OPT).withDescription(REPROCESS_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(META_OPT).withDescription(META_DESC)
                .hasArg().withArgName(META_ARG_NAME).create());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        if (!line.hasOption(DB_URL_OPT))
            throw new ConfigurationException("Must provide a database to use the tracking filter");

        if (line.hasOption(TABLE_OPT)) tableName = line.getOptionValue(TABLE_OPT);

        createTable = line.hasOption(CREATE_TABLE_OPT);
        processAllObjects = line.hasOption(REPROCESS_OPT);

        if (line.hasOption(META_OPT)) metaTags = Arrays.asList(line.getOptionValue(META_OPT).split(","));

        // Initialize a DB connection pool
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(line.getOptionValue(DB_URL_OPT));
        if (line.hasOption(DB_DRIVER_OPT)) ds.setDriverClassName(line.getOptionValue(DB_DRIVER_OPT));
        ds.setUsername(line.getOptionValue(DB_USER_OPT));
        ds.setPassword(line.getOptionValue(DB_PASSWORD_OPT));
        ds.setMaxActive(200);
        ds.setMaxOpenPreparedStatements(180);
        dataSource = ds;
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.notNull(dataSource, "a data source must be specified");
        template = new JdbcTemplate(dataSource);

        if (!tableName.matches("^[_a-zA-Z][_a-zA-Z0-9]*$"))
            throw new ConfigurationException(MessageFormat.format("invalid table name: ", tableName));

        // Make sure the table exists (create if appropriate)
        Connection con = null;
        try {
            con = dataSource.getConnection();
            boolean tableExists, multipleTables;
            PreparedStatement st = null;
            try {
                st = con.prepareStatement(SQL_FIND_TABLE);
                st.setString(1, tableName.toUpperCase());
                ResultSet rs = null;
                try {
                    rs = st.executeQuery();
                    tableExists = rs.next();
                    multipleTables = rs.next();
                } finally {
                    try {
                        if (rs != null) rs.close();
                    } catch (Throwable t) {
                        l4j.warn("could not close resource", t);
                    }
                }
            } finally {
                try {
                    if (st != null) st.close();
                } catch (Throwable t) {
                    l4j.warn("could not close resource", t);
                }
            }

            if (!tableExists) {
                if (createTable) {
                    st = null;
                    try {
                        st = con.prepareStatement(String.format(createDdl(), tableName));
                        st.executeUpdate();
                    } finally {
                        try {
                            if (st != null) st.close();
                        } catch (Throwable t) {
                            l4j.warn("could not close resource", t);
                        }
                    }
                } else {
                    throw new ConfigurationException(String.format("tracking table (%s) does not exist", tableName));
                }
            } else if (multipleTables) {
                throw new ConfigurationException("multiple tracking tables found (choose a name unique across all schemas)");
            }
        } catch (Exception e) {
            throw new ConfigurationException("unable to access tracking table: " + e.getMessage(), e);
        } finally {
            try {
                if (con != null) con.close();
            } catch (Throwable t) {
                l4j.warn("could not close resource", t);
            }
        }

        if (metaTags != null) {
            for (String tag : metaTags) {
                if (!tag.matches("^[_a-zA-Z][_a-zA-Z0-9]*$"))
                    throw new ConfigurationException(MessageFormat.format("only metadata with valid SQL column names can be recorded ({0} is invalid)", tag));
            }
        }
    }

    @Override
    public void filter(final SyncObject obj) {
        String sourceId = obj.getSourceIdentifier();
        boolean statusExists = false;
        Map<String, String> metaValues = new HashMap<String, String>();
        Map<StatusProperty, Object> propertyMap = new HashMap<StatusProperty, Object>();

        try {
            SqlRowSet rowSet = getExistingStatus(sourceId);
            if (rowSet.next()) {
                // status exists for this object
                statusExists = true;
                String targetId = rowSet.getString("target_id");
                if (targetId != null && targetId.trim().length() > 0) obj.setTargetIdentifier(targetId);

                // if the object is already complete, short-circuit the sync here (skip the object)
                if (SKIP_STATUSES.contains(rowSet.getString("status")) && !processAllObjects) {
                    LogMF.debug(l4j, "{0} is marked complete; skipping", sourceId);
                    return;
                }
            }

            // get metadata values before processing (what if we're deleting the object?)
            for (String name : metaTags) {
                String metaValue = obj.getMetadata().getUserMetadataValue(name);
                if (metaValue == null && obj.getMetadata() instanceof AtmosMetadata) // try system meta too
                    metaValue = ((AtmosMetadata) obj.getMetadata()).getSystemMetadataValue(name);
                if (metaValue != null) metaValues.put(name, metaValue);
            }

            // sync started; update tracking table
            propertyMap.put(StatusProperty.status, IN_PROCESS_STATUS);
            propertyMap.put(StatusProperty.started_at, System.currentTimeMillis());
            propertyMap.put(StatusProperty.meta, metaValues);

            if (statusExists) statusUpdate(sourceId, propertyMap);
            else statusInsert(sourceId, propertyMap);

            // process object
            getNext().filter(obj);

            // sync completed successfully; update tracking table
            propertyMap.put(StatusProperty.status, COMPLETE_STATUS);
            propertyMap.put(StatusProperty.completed_at, System.currentTimeMillis());
            propertyMap.put(StatusProperty.target_id, obj.getTargetIdentifier());

            statusUpdate(sourceId, propertyMap);

        } catch (RuntimeException e) {

            // sync failed; update tracking table
            String message = SyncUtil.summarize(e);
            if (message.length() > MESSAGE_COLUMN_SIZE) message = message.substring(0, MESSAGE_COLUMN_SIZE);

            propertyMap.put(StatusProperty.status, ERROR_STATUS);
            propertyMap.put(StatusProperty.message, message);
            propertyMap.put(StatusProperty.target_id, obj.getTargetIdentifier());

            statusUpdate(sourceId, propertyMap);

            throw e;
        }
    }

    @Override
    public SyncObject reverseFilter(final SyncObject obj) {
        final SyncObject targetObj = getNext().reverseFilter(obj);

        final boolean statusExists = getExistingStatus(obj.getSourceIdentifier()).next();
        final Map<StatusProperty, Object> propertyMap = new HashMap<StatusProperty, Object>();
        propertyMap.put(StatusProperty.target_id, targetObj.getSourceIdentifier());

        return new ValidatingObject(targetObj, new Validator() {
            @Override
            public void validate(String md5Hex) {
                if (obj.getMd5Hex(true).equals(md5Hex)) {
                    // MD5 data verification successful
                    propertyMap.put(StatusProperty.status, VERIFIED_STATUS);
                    propertyMap.put(StatusProperty.verified_at, System.currentTimeMillis());
                } else {
                    // MD5 data verification failed
                    propertyMap.put(StatusProperty.status, ERROR_STATUS);
                    propertyMap.put(StatusProperty.message,
                            String.format("MD5 verification failed (%s != %s)", obj.getMd5Hex(true), md5Hex));
                }

                if (statusExists) statusUpdate(obj.getSourceIdentifier(), propertyMap);
                else statusInsert(obj.getSourceIdentifier(), propertyMap);
            }

            @Override
            public void error(Exception e) {
                String message = SyncUtil.summarize(e);
                if (message.length() > MESSAGE_COLUMN_SIZE) message = message.substring(0, MESSAGE_COLUMN_SIZE);

                propertyMap.put(StatusProperty.status, ERROR_STATUS);
                propertyMap.put(StatusProperty.message, message);

                if (statusExists) statusUpdate(obj.getSourceIdentifier(), propertyMap);
                else statusInsert(obj.getSourceIdentifier(), propertyMap);
            }
        });
    }

    @Override
    public String getName() {
        return "Tracking Filter";
    }

    @Override
    public String getDocumentation() {
        return "Tracks sync status for each object in a database table that includes source ID, target ID, status (Complete or Error), timestamp and an error message. Additional columns can be populated with object metadata provided the table has been created with said columns.";
    }

    protected SqlRowSet getExistingStatus(final String sourceId) {
        return time(new Function<SqlRowSet>() {
            @Override
            public SqlRowSet call() {
                return template.queryForRowSet(String.format(SQL_STATUS_QUERY, tableName), sourceId);
            }
        }, OPERATION_STATUS_QUERY);
    }

    protected String createDdl() {
        StringBuilder ddl = new StringBuilder();
        ddl.append("create table ").append(tableName).append(" (\n");
        ddl.append("source_id varchar(1500) not null,\n");
        ddl.append("target_id varchar(1500),\n");
        ddl.append("started_at timestamp null,\n");
        ddl.append("completed_at timestamp null,\n");
        ddl.append("verified_at timestamp null,\n");
        ddl.append("status varchar(32) not null,\n");
        for (String name : metaTags) {
            ddl.append(name).append(" varchar(1024),\n");
        }
        ddl.append("message varchar(").append(MESSAGE_COLUMN_SIZE).append(")\n");
        ddl.append(")");

        return ddl.toString();
    }

    @SuppressWarnings("unchecked")
    protected void statusInsert(String sourceId, Map<StatusProperty, Object> properties) {
        final List<Object> params = new ArrayList<Object>();
        final StringBuilder sql = new StringBuilder("insert into ").append(tableName).append(" (source_id");
        params.add(sourceId);
        for (StatusProperty property : new StatusProperty[]
                {StatusProperty.target_id, StatusProperty.status, StatusProperty.message}) {
            if (properties.containsKey(property)) {
                sql.append(", ").append(property.toString());
                params.add(properties.get(property));
            }
        }
        for (StatusProperty property : new StatusProperty[]
                {StatusProperty.started_at, StatusProperty.completed_at, StatusProperty.verified_at}) {
            if (properties.containsKey(property)) {
                sql.append(", ").append(property.toString());
                params.add(new Timestamp((Long) properties.get(property)));
            }
        }
        if (properties.containsKey(StatusProperty.meta)) {
            Map<String, String> meta = (Map<String, String>) properties.get(StatusProperty.meta);
            for (String key : meta.keySet()) {
                sql.append(", ").append(key);
                params.add(meta.get(key));
            }
        }
        sql.append(") values (?");
        for (int i = 1; i < params.size(); i++) {
            sql.append(", ?");
        }
        sql.append(")");
        time(new Function<Void>() {
            @Override
            public Void call() {
                template.update(sql.toString(), params.toArray());
                return null;
            }
        }, OPERATION_STATUS_UPDATE);
    }

    @SuppressWarnings("unchecked")
    protected void statusUpdate(String sourceId, Map<StatusProperty, Object> properties) {
        final List<Object> params = new ArrayList<Object>();
        final StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");

        for (StatusProperty property : new StatusProperty[]
                {StatusProperty.target_id, StatusProperty.status, StatusProperty.message}) {
            if (properties.containsKey(property)) {
                if (params.size() > 0) sql.append(", ");
                sql.append(property.toString()).append("=?");
                params.add(properties.get(property));
            }
        }
        for (StatusProperty property : new StatusProperty[]
                {StatusProperty.started_at, StatusProperty.completed_at, StatusProperty.verified_at}) {
            if (properties.containsKey(property)) {
                if (params.size() > 0) sql.append(", ");
                sql.append(property.toString()).append("=?");
                params.add(new Timestamp((Long) properties.get(property)));
            }
        }
        if (properties.containsKey(StatusProperty.meta)) {
            Map<String, String> meta = (Map<String, String>) properties.get(StatusProperty.meta);
            for (String key : meta.keySet()) {
                if (params.size() > 0) sql.append(", ");
                sql.append(key).append("=?");
                params.add(meta.get(key));
            }
        }
        sql.append(" where source_id=?");
        params.add(sourceId);

        time(new Function<Void>() {
            @Override
            public Void call() {
                template.update(sql.toString(), params.toArray());
                return null;
            }
        }, OPERATION_STATUS_UPDATE);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isCreateTable() {
        return createTable;
    }

    public void setCreateTable(boolean createTable) {
        this.createTable = createTable;
    }

    public boolean isProcessAllObjects() {
        return processAllObjects;
    }

    public void setProcessAllObjects(boolean processAllObjects) {
        this.processAllObjects = processAllObjects;
    }

    public List<String> getMetaTags() {
        return metaTags;
    }

    public void setMetaTags(List<String> metaTags) {
        this.metaTags = metaTags;
    }

    public enum StatusProperty {
        target_id, started_at, completed_at, verified_at, status, message, meta
    }

    public interface Validator {
        void validate(String md5Hex);

        void error(Exception e);
    }

    public static class ValidatingObject implements SyncObject {
        private SyncObject delegate;
        private Validator validator;

        public ValidatingObject(SyncObject delegate, Validator validator) {
            this.delegate = delegate;
            this.validator = validator;
        }

        @Override
        public Object getRawSourceIdentifier() {
            return delegate.getRawSourceIdentifier();
        }

        @Override
        public String getSourceIdentifier() {
            return delegate.getSourceIdentifier();
        }

        @Override
        public String getRelativePath() {
            return delegate.getRelativePath();
        }

        @Override
        public boolean isDirectory() {
            return delegate.isDirectory();
        }

        @Override
        public String getTargetIdentifier() {
            return delegate.getTargetIdentifier();
        }

        @Override
        public SyncMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public boolean requiresPostStreamMetadataUpdate() {
            return delegate.requiresPostStreamMetadataUpdate();
        }

        @Override
        public void setTargetIdentifier(String targetIdentifier) {
            delegate.setTargetIdentifier(targetIdentifier);
        }

        @Override
        public void setMetadata(SyncMetadata metadata) {
            delegate.setMetadata(metadata);
        }

        @Override
        public InputStream getInputStream() {
            return delegate.getInputStream();
        }

        @Override
        public long getBytesRead() {
            return delegate.getBytesRead();
        }

        @Override
        public String getMd5Hex(boolean forceRead) {
            try {
                String md5Hex = delegate.getMd5Hex(forceRead);
                validator.validate(md5Hex);
                return md5Hex;
            } catch (RuntimeException e) {
                validator.error(e);
                throw e;
            }
        }
    }
}
