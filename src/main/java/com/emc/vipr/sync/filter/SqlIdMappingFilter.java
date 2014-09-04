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

import com.emc.vipr.sync.model.BasicMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.JdbcWrapper;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.Timeable;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Maps IDs from source and targets into a database.  Note that while it
 * is possible to initialize and use this plugin through the command line, the
 * recommended approach is to initialize the application using Spring and
 * provide a DataSource directly.  When using the command line, the c3p0 engine
 * will be used to create a suitable DataSource.
 *
 * @author cwikj
 */
public class SqlIdMappingFilter extends SyncFilter implements DisposableBean {
    private static final Logger l4j = Logger.getLogger(SqlIdMappingFilter.class);

    public static final String ACTIVATION_NAME = "sql-id-mapping";

    public static final String JDBC_URL_OPT = "id-map-jdbc-url";
    public static final String JDBC_URL_DESC = "The URL to the database";
    public static final String JDBC_URL_ARG_NAME = "jdbc-url";

    public static final String JDBC_DRIVER_OPT = "id-map-jdbc-driver-class";
    public static final String JDBC_DRIVER_DESC = "The JDBC database driver class";
    public static final String JDBC_DRIVER_ARG_NAME = "java-class-name";

    public static final String JDBC_USER_OPT = "id-map-user";
    public static final String JDBC_USER_DESC = "The database userid";
    public static final String JDBC_USER_ARG_NAME = "userid";

    public static final String JDBC_PASSWORD_OPT = "id-map-password";
    public static final String JDBC_PASSWORD_DESC = "The database password";
    public static final String JDBC_PASSWORD_ARG_NAME = "password";

    public static final String JDBC_SELECT_OPT = "id-map-select-sql";
    public static final String JDBC_SELECT_DESC = "The SQL statement to lookup a target ID, e.g. \"SELECT dest FROM id_map WHERE src=:source_id\".  It is assumed that the query will have one argument: the source (:source_id).  If the query does not return a row, a new object will be created in the target.  Optional.  If omitted, all objects will be created new in the target.";
    public static final String JDBC_SELECT_ARG_NAME = "sql-statement";

    public static final String JDBC_MAP_OPT = "id-map-insert-sql";
    public static final String JDBC_MAP_DESC = "The SQL statement to insert mapped IDs, e.g. \"INSERT INTO id_map(src,dest) VALUES(:source_id,:dest_id)\".  It is assumed that the query will have two arguments: source (:source_id) and target (:dest_id) unless id-map-add-metadata is specified, in which case it must also contain a parameter for each tag in the tag-list.";
    public static final String JDBC_MAP_ARG_NAME = "sql-statement";

    public static final String JDBC_ERROR_OPT = "id-map-error-sql";
    public static final String JDBC_ERROR_DESC = "The SQL statement to insert error messages, e.g. \"INSERT INTO id_error(src,msg) VALUES(:source_id,:error_msg)\".  This is optional.  If not specified, errors will not be logged to the database.";
    public static final String JDBC_ERROR_ARG_NAME = "sql-statement";

    public static final String JDBC_ADD_META_OPT = "id-map-add-metadata";
    public static final String JDBC_ADD_META_DESC = "A comma-separated list of metadata names whose values should be added as parameters to the insert-sql statement, e.g. \"size,mtime,ctime\".  This is optional.  If specified, each value will be pulled from the source object and added to the id-map-insert-sql statement as named JDBC parameters using the name of the tag (e.g. :size, :mtime, :ctime).  Note that the insert-sql must then contain these parameters.";
    public static final String JDBC_ADD_META_ARG_NAME = "tag-list";

    private static final String SOURCE_PARAM = "source_id";
    private static final String DEST_PARAM = "dest_id";
    private static final String ERROR_PARAM = "error_msg";

    // timed operations
    private static final String OPERATION_MAPPING_SELECT = "IdMappingSelect";
    private static final String OPERATION_MAPPING_INSERT = "IdMappingInsert";
    private static final String OPERATION_ERROR_INSERT = "ErrorInsert";
    private static final String OPERATION_TOTAL = "TotalTime";

    private DataSource dataSource;
    private JdbcWrapper jdbcWrapper;
    private boolean hardThreadedConnections = false;
    private String mapQuery;
    private String errorQuery;
    private String selectQuery;
    private String[] metaTags;

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_URL_OPT).withDescription(JDBC_URL_DESC)
                .hasArg().withArgName(JDBC_URL_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_DRIVER_OPT).withDescription(JDBC_DRIVER_DESC)
                .hasArg().withArgName(JDBC_DRIVER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_USER_OPT).withDescription(JDBC_USER_DESC)
                .hasArg().withArgName(JDBC_USER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_PASSWORD_OPT).withDescription(JDBC_PASSWORD_DESC)
                .hasArg().withArgName(JDBC_PASSWORD_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_MAP_OPT).withDescription(JDBC_MAP_DESC)
                .hasArg().withArgName(JDBC_MAP_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_SELECT_OPT).withDescription(JDBC_SELECT_DESC)
                .hasArg().withArgName(JDBC_SELECT_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_ERROR_OPT).withDescription(JDBC_ERROR_DESC)
                .hasArg().withArgName(JDBC_ERROR_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_ADD_META_OPT).withDescription(JDBC_ADD_META_DESC)
                .hasArg().withArgName(JDBC_ADD_META_ARG_NAME).create());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        if (!line.hasOption(JDBC_URL_OPT) || !line.hasOption(JDBC_DRIVER_OPT) || !line.hasOption(JDBC_USER_OPT)
                || !line.hasOption(JDBC_PASSWORD_OPT) || !line.hasOption(JDBC_MAP_OPT))
            throw new ConfigurationException(String.format("these options are required for %s: %s, %s, %s, %s, %s",
                    ACTIVATION_NAME, JDBC_URL_OPT, JDBC_DRIVER_OPT, JDBC_USER_OPT, JDBC_PASSWORD_OPT, JDBC_MAP_OPT));

        mapQuery = line.getOptionValue(JDBC_MAP_OPT);
        if (line.hasOption(JDBC_ERROR_OPT)) {
            errorQuery = line.getOptionValue(JDBC_ERROR_OPT);
        }
        if (line.hasOption(JDBC_SELECT_OPT)) {
            selectQuery = line.getOptionValue(JDBC_SELECT_OPT);
        }

        if (line.hasOption(JDBC_ADD_META_OPT)) {
            metaTags = line.getOptionValue(JDBC_ADD_META_OPT).split(",");
            for (String tag : metaTags) {
                if (!tag.matches("^[_a-zA-Z][_a-zA-Z0-9]*$"))
                    throw new ConfigurationException(MessageFormat.format("Only metadata with valid JDBC parameter names can be recorded ({0} is invalid)", tag));
            }
        }

        // Initialize a c3p0 pool
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass(line.getOptionValue(JDBC_DRIVER_OPT));
            cpds.setJdbcUrl(line.getOptionValue(JDBC_URL_OPT));
            cpds.setUser(line.getOptionValue(JDBC_USER_OPT));
            cpds.setPassword(line.getOptionValue(JDBC_PASSWORD_OPT));
        } catch (PropertyVetoException e) {
            throw new ConfigurationException("Unable to initialize JDBC driver: " + e.getMessage(), e);
        }
        cpds.setMaxStatements(180);
        dataSource = cpds;

        jdbcWrapper = new JdbcWrapper(dataSource);
        jdbcWrapper.setHardThreaded(hardThreadedConnections);
    }

    @Override
    public void validateChain(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.notNull(dataSource, "A data source must be specified");
        Assert.hasText(mapQuery, "The query to map IDs is required");

        // Try getting a connection
        try {
            Connection con = dataSource.getConnection();
            con.close();
        } catch (Exception e) {
            throw new ConfigurationException("Unable to connect to JDBC database: " + e.getMessage(), e);
        }
    }

    @Override
    public void filter(SyncObject obj) {
        timeOperationStart(OPERATION_TOTAL);
        String sourceId = obj.getSourceIdentifier();
        try {
            boolean alreadyMapped = false;
            if (selectQuery != null) {
                // Check to see if ID mapping exists.
                final Map<String, String> params = Collections.singletonMap(SOURCE_PARAM, sourceId);
                // Could use as update instead of mapping
                if (selectQuery.toLowerCase().startsWith("update") || selectQuery.toLowerCase().startsWith("insert")) {
                    time(new Timeable<Void>() {
                        @Override
                        public Void call() {
                            jdbcWrapper.executeUpdate(selectQuery, params);
                            return null;
                        }
                    }, OPERATION_MAPPING_SELECT);
                } else {
                    try {
                        String id = time(new Timeable<String>() {
                            @Override
                            public String call() {
                                return jdbcWrapper.executeQueryForObject(selectQuery, params, String.class);
                            }
                        }, OPERATION_MAPPING_SELECT);

                        if (id != null) {
                            // Looks like an ID already exists.
                            obj.setTargetIdentifier(id.trim());
                            alreadyMapped = true;
                        }
                    } catch (IncorrectResultSizeDataAccessException e) {
                        // OK -- Row not found.
                    }

                }
            }

            getNext().filter(obj);

            String targetId = obj.getTargetIdentifier();
            if (!alreadyMapped) {
                final Map<String, String> params = new HashMap<>();
                params.put(SOURCE_PARAM, sourceId);
                params.put(DEST_PARAM, targetId);
                if (metaTags != null) {
                    SyncMetadata meta = obj.getMetadata();
                    if (meta == null) meta = new BasicMetadata();
                    for (String tag : metaTags) {
                        String value = meta.getUserMetadataProp(tag);
                        if (value == null) value = meta.getSystemMetadataProp(tag);
                        params.put(tag, value);
                    }
                }
                time(new Timeable<Void>() {
                    @Override
                    public Void call() {
                        jdbcWrapper.executeUpdate(mapQuery, params);
                        return null;
                    }
                }, OPERATION_MAPPING_INSERT);
            }

            timeOperationComplete(OPERATION_TOTAL);

        } catch (RuntimeException e) {
            if (errorQuery != null) {
                try {
                    final Map<String, String> params = new HashMap<>();
                    params.put(SOURCE_PARAM, sourceId);
                    if (e.getClass().equals(RuntimeException.class) && e.getCause() != null) {
                        params.put(ERROR_PARAM, e.getCause().toString());
                    } else {
                        params.put(ERROR_PARAM, e.toString());
                    }
                    time(new Timeable<Void>() {
                        @Override
                        public Void call() {
                            jdbcWrapper.executeUpdate(errorQuery, params);
                            return null;
                        }
                    }, OPERATION_ERROR_INSERT);
                } catch (Exception e1) {
                    l4j.error("Error inserting error message: " + e1.getMessage(), e1);
                }

            }

            timeOperationFailed(OPERATION_TOTAL);

            // re-throw
            throw e;
        }
    }

    @Override
    public void destroy() throws Exception {
        jdbcWrapper.closeHardThreadedConnections();
    }

    @Override
    public String getName() {
        return "Database ID Mapper";
    }

    @Override
    public String getDocumentation() {
        return "Uses a JDBC database connection to record the mapped source and target IDs.  Can optionally log errors " +
                "to the database as well.";
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public boolean isHardThreadedConnections() {
        return hardThreadedConnections;
    }

    /**
     * Sets whether to use hard-threaded connections. That means each thread will get a dedicated connection that is
     * never closed until {@link #destroy()} is called. Default is false.
     */
    public void setHardThreadedConnections(boolean hardThreadedConnections) {
        this.hardThreadedConnections = hardThreadedConnections;
    }

    /**
     * Gets the query to map IDs inside the database
     *
     * @return the mapQuery
     */
    public String getMapQuery() {
        return mapQuery;
    }

    /**
     * Sets the query to map IDs inside the database
     *
     * @param mapQuery the mapQuery to set
     */
    public void setMapQuery(String mapQuery) {
        this.mapQuery = mapQuery;
    }

    /**
     * Gets the query to map errors inside the database
     *
     * @return the errorQuery
     */
    public String getErrorQuery() {
        return errorQuery;
    }

    /**
     * Sets the query to map error messages inside the database
     *
     * @param errorQuery the errorQuery to set
     */
    public void setErrorQuery(String errorQuery) {
        this.errorQuery = errorQuery;
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

    public String[] getMetaTags() {
        return metaTags;
    }

    public void setMetaTags(String[] metaTags) {
        this.metaTags = metaTags;
    }
}
