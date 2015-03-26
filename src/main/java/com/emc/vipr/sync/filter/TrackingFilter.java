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
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.Function;
import com.emc.vipr.sync.util.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.Assert;

import javax.sql.DataSource;
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
    private static final String SQL_STATUS_QUERY = "select source_id, target_id, status from %s where source_id = ?";

    private static final String COMPLETE_STATUS = "Complete";
    private static final String ERROR_STATUS = "Error";

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
        ds.setMaxTotal(200);
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
        final String sourceId = obj.getSourceIdentifier();
        boolean statusExists = false;
        final Map<String, String> metaValues = new HashMap<String, String>();

        try {
            SqlRowSet rowSet = time(new Function<SqlRowSet>() {
                @Override
                public SqlRowSet call() {
                    return template.queryForRowSet(String.format(SQL_STATUS_QUERY, tableName), sourceId);
                }
            }, OPERATION_STATUS_QUERY);
            if (rowSet.next()) {
                // status exists for this object
                statusExists = true;
                String targetId = rowSet.getString("target_id");
                if (targetId != null && targetId.trim().length() > 0) obj.setTargetIdentifier(targetId);

                // if the object is already complete, short-circuit the sync here (skip the object)
                if (COMPLETE_STATUS.equals(rowSet.getString("status")) && !processAllObjects) {
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

            // process object
            getNext().filter(obj);

            // sync completed successfully; update tracking table
            final boolean finalStatusExists = statusExists;
            time(new Function<Void>() {
                @Override
                public Void call() {
                    template.update(finalStatusExists ? createStatusUpdateSql() : createStatusInsertSql(),
                            createStatusParameters(obj, metaValues, COMPLETE_STATUS, null));
                    return null;
                }
            }, OPERATION_STATUS_UPDATE);

        } catch (final RuntimeException e) {

            // sync failed; update tracking table
            final boolean finalStatusExists = statusExists;
            time(new Function<Void>() {
                @Override
                public Void call() {
                    template.update(finalStatusExists ? createStatusUpdateSql() : createStatusInsertSql(),
                            createStatusParameters(obj, metaValues, ERROR_STATUS, e.getMessage()));
                    return null;
                }
            }, OPERATION_STATUS_UPDATE);

            throw e;
        }
    }

    @Override
    public String getName() {
        return "Tracking Filter";
    }

    @Override
    public String getDocumentation() {
        return "Tracks sync status for each object in a database table that includes source ID, target ID, status (Complete or Error), timestamp and an error message. Additional columns can be populated with object metadata provided the table has been created with said columns.";
    }

    protected String createDdl() {
        StringBuilder ddl = new StringBuilder();
        ddl.append("create table ").append(tableName).append(" (\n");
        ddl.append("source_id varchar(512) primary key not null,\n");
        ddl.append("target_id varchar(512),\n");
        ddl.append("synced_at timestamp not null,\n");
        ddl.append("status varchar(32) not null,\n");
        ddl.append("message varchar(1024),\n");
        for (String name : metaTags) {
            ddl.append(name).append(" varchar(1024),\n");
        }
        ddl.append("check (status in ('Complete', 'Error'))\n");
        ddl.append(")");

        return ddl.toString();
    }

    protected String createStatusInsertSql() {
        List<String> names = new ArrayList<String>();
        names.add("synced_at");
        names.add("status");
        names.add("message");
        names.addAll(metaTags);

        StringBuilder sql = new StringBuilder("insert into ").append(tableName).append(" (target_id");
        for (String name : names) {
            sql.append(", ").append(name);
        }
        sql.append(", source_id) values (?");
        for (String name : names) {
            sql.append(", ?");
        }
        sql.append(", ?)");

        return sql.toString();
    }

    protected String createStatusUpdateSql() {
        List<String> names = new ArrayList<String>();
        names.add("synced_at");
        names.add("status");
        names.add("message");
        names.addAll(metaTags);

        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set target_id=?");
        for (String name : names) {
            sql.append(", ").append(name).append("=?");
        }
        sql.append(" where source_id=?");

        return sql.toString();
    }

    protected Object[] createStatusParameters(SyncObject<?> obj, Map<String, String> metaValues, String status, String message) {
        List<Object> params = new ArrayList<Object>();
        params.add(obj.getTargetIdentifier());
        params.add(new Timestamp(System.currentTimeMillis()));
        params.add(status);
        params.add(message);
        for (String name : metaTags) {
            params.add(metaValues.get(name));
        }
        params.add(obj.getSourceIdentifier());

        return params.toArray();
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
}
