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

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.DirectoryEntry;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.ServiceInformation;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.ListDirectoryRequest;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.object.AtmosSyncObject;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Reads objects from an Atmos system.
 */
public class AtmosSource extends SyncSource<AtmosSyncObject> {
    private static final Logger log = LoggerFactory.getLogger(AtmosSource.class);

    public static final String SOURCE_OIDLIST_OPTION = "source-oid-list";
    public static final String SOURCE_OIDLIST_DESC = "The file containing the list of OIDs to copy (newline separated).  Use - to read the list from standard input.  Not compatible with source-namespace";
    public static final String SOURCE_OIDLIST_ARG_NAME = "filename";

    public static final String SOURCE_NAMELIST_OPTION = "source-name-list";
    public static final String SOURCE_NAMELIST_DESC = "The file containing the list of namespace paths to copy (newline separated).  Use - to read the list from standard input.  Not compatible with source-namespace";
    public static final String SOURCE_NAMELIST_ARG_NAME = "filename";

    public static final String SOURCE_SQLQUERY_OPTION = "source-sql-query";
    public static final String SOURCE_SQLQUERY_DESC = "The SQL query to use to select the OIDs to copy from a database.  This query is assumed to return a raw OID (not a URL) as the first column in each result.  Not compatible with any other source options.  If specified, all of the query-* options must also be specified.";
    public static final String SOURCE_SQLQUERY_ARG_NAME = "sql-query";

    public static final String JDBC_URL_OPT = "query-jdbc-url";
    public static final String JDBC_URL_DESC = "The URL to the database (used in conjunction with source-sql-query)";
    public static final String JDBC_URL_ARG_NAME = "jdbc-url";

    public static final String JDBC_DRIVER_OPT = "query-jdbc-driver-class";
    public static final String JDBC_DRIVER_DESC = "The JDBC database driver class (used in conjunction with source-sql-query)";
    public static final String JDBC_DRIVER_ARG_NAME = "java-class-name";

    public static final String JDBC_USER_OPT = "query-user";
    public static final String JDBC_USER_DESC = "The database userid (used in conjunction with source-sql-query)";
    public static final String JDBC_USER_ARG_NAME = "userid";

    public static final String JDBC_PASSWORD_OPT = "query-password";
    public static final String JDBC_PASSWORD_DESC = "The database password (used in conjunction with source-sql-query)";
    public static final String JDBC_PASSWORD_ARG_NAME = "password";

    public static final String DELETE_TAGS_OPT = "remove-tags-on-delete";
    public static final String DELETE_TAGS_DESC = "When used with the DeleteSourceTarget or when specifying --delete-source, this will attempt to remove listable tags from objects before deleting them.";

    // timed operations
    public static final String OPERATION_LIST_DIRECTORY = "AtmosListDirectory";
    public static final String OPERATION_GET_ALL_META = "AtmosGetAllMeta";
    public static final String OPERATION_GET_OBJECT_INFO = "AtmosGetObjectInfo";
    public static final String OPERATION_GET_OBJECT_STREAM = "AtmosGetObjectStream";
    public static final String OPERATION_GET_USER_META = "AtmosGetUserMeta";
    public static final String OPERATION_DELETE_USER_META = "AtmosDeleteUserMeta";
    public static final String OPERATION_DELETE_OBJECT = "AtmosDeleteObject";

    private List<URI> endpoints;
    private String uid;
    private String secret;
    private AtmosApi atmos;
    private DataSource dataSource;

    private String namespaceRoot;
    private String oidFile;
    private String query;
    private String nameFile;
    private boolean deleteTags = false;

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(SOURCE_OIDLIST_OPTION).desc(SOURCE_OIDLIST_DESC)
                .hasArg().argName(SOURCE_OIDLIST_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(SOURCE_NAMELIST_OPTION).desc(SOURCE_NAMELIST_DESC)
                .hasArg().argName(SOURCE_NAMELIST_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(SOURCE_SQLQUERY_OPTION).desc(SOURCE_SQLQUERY_DESC)
                .hasArg().argName(SOURCE_SQLQUERY_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(JDBC_URL_OPT).desc(JDBC_URL_DESC)
                .hasArg().argName(JDBC_URL_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(JDBC_DRIVER_OPT).desc(JDBC_DRIVER_DESC)
                .hasArg().argName(JDBC_DRIVER_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(JDBC_USER_OPT).desc(JDBC_USER_DESC)
                .hasArg().argName(JDBC_USER_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(JDBC_PASSWORD_OPT).desc(JDBC_PASSWORD_DESC)
                .hasArg().argName(JDBC_PASSWORD_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(DELETE_TAGS_OPT).desc(DELETE_TAGS_DESC).build());
        return opts;
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.startsWith(AtmosUtil.URI_PREFIX);
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        AtmosUtil.AtmosUri atmosUri = AtmosUtil.parseUri(sourceUri);
        endpoints = atmosUri.endpoints;
        uid = atmosUri.uid;
        secret = atmosUri.secret;
        namespaceRoot = atmosUri.rootPath;

        if (line.hasOption(SOURCE_OIDLIST_OPTION))
            oidFile = line.getOptionValue(SOURCE_OIDLIST_OPTION);

        if (line.hasOption(SOURCE_NAMELIST_OPTION))
            nameFile = line.getOptionValue(SOURCE_NAMELIST_OPTION);

        if (line.hasOption(SOURCE_SQLQUERY_OPTION)) {
            query = line.getOptionValue(SOURCE_SQLQUERY_OPTION);

            // Initialize a connection pool
            BasicDataSource ds = new BasicDataSource();
            ds.setUrl(line.getOptionValue(JDBC_URL_OPT));
            if (line.hasOption(JDBC_DRIVER_OPT)) ds.setDriverClassName(line.getOptionValue(JDBC_DRIVER_OPT));
            ds.setUsername(line.getOptionValue(JDBC_USER_OPT));
            ds.setPassword(line.getOptionValue(JDBC_PASSWORD_OPT));
            ds.setMaxActive(200);
            ds.setMaxOpenPreparedStatements(180);
            setDataSource(ds);
        }

        deleteTags = line.hasOption(DELETE_TAGS_OPT);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (atmos == null) {
            if (endpoints == null || uid == null || secret == null)
                throw new ConfigurationException("Must specify endpoints, uid and secret key");
            atmos = new AtmosApiClient(new AtmosConfig(uid, secret, endpoints.toArray(new URI[endpoints.size()])));
        }

        // Check authentication
        ServiceInformation info = atmos.getServiceInformation();
        log.info("Connected to Atmos {} on {}", info.getAtmosVersion(), endpoints);

        boolean namespace = namespaceRoot != null;
        boolean objectlist = oidFile != null;
        boolean namelist = nameFile != null;
        boolean sqllist = query != null;

        int optCount = 0;
        if (namespace) optCount++;
        if (objectlist) optCount++;
        if (namelist) optCount++;
        if (sqllist) optCount++;

        if (optCount > 1) {
            throw new ConfigurationException(MessageFormat.format(
                    "Only one of ({0}, --{1}, --{2}, --{3}) is allowed",
                    "namespace-path", SOURCE_OIDLIST_OPTION,
                    SOURCE_NAMELIST_OPTION, SOURCE_SQLQUERY_OPTION));
        }
        if (optCount < 1) {
            throw new ConfigurationException(MessageFormat.format(
                    "One of ({0}, --{1}, --{2}, --{3}) must be specified",
                    "namespace-path", SOURCE_OIDLIST_OPTION,
                    SOURCE_NAMELIST_OPTION, SOURCE_SQLQUERY_OPTION));
        }

        if (objectlist && !"-".equals(oidFile)) {
            // Verify file
            File f = new File(oidFile);
            if (!f.exists()) {
                throw new ConfigurationException(MessageFormat.format("The OID list file {0} does not exist", oidFile));
            }
        }

        if (namelist && !"-".equals(nameFile)) {
            // Verify file
            File f = new File(nameFile);
            if (!f.exists()) {
                throw new ConfigurationException(MessageFormat.format("The pathname list file {0} does not exist", nameFile));
            }
        }

        if (namespace) {
            if (!namespaceRoot.startsWith("/")) namespaceRoot = "/" + namespaceRoot;
            if (!namespaceRoot.equals("/")) {
                namespaceRoot = namespaceRoot.replaceFirst("/$", "");

                // does namespaceRoot exist?
                try {
                    Metadata typeMeta = atmos.getSystemMetadata(new ObjectPath(namespaceRoot)).get(AtmosUtil.TYPE_KEY);
                    if (AtmosUtil.DIRECTORY_TYPE.equals(typeMeta.getValue()))
                        namespaceRoot += "/";
                } catch (AtmosException e) {
                    if (e.getErrorCode() == 1003)
                        throw new ConfigurationException("specified path does not exist in the cloud");
                    throw e;
                }
            }
        }
    }

    @Override
    public String getName() {
        return "Atmos Source";
    }

    @Override
    public String getDocumentation() {
        return "The Atmos source plugin is triggered by the source pattern:\n" +
                AtmosUtil.PATTERN_DESC + "\n" +
                "Note that the uid should be the 'full token ID' including the " +
                "subtenant ID and the uid concatenated by a slash\n" +
                "If you want to software load balance across multiple hosts, " +
                "you can provide a comma-delimited list of hostnames or IPs " +
                "in the host part of the URI.";
    }

    @Override
    public Iterator<AtmosSyncObject> iterator() {
        if (namespaceRoot != null) {
            ObjectPath objectPath = new ObjectPath(namespaceRoot);
            return Collections.singletonList(
                    new AtmosSyncObject(this, atmos, objectPath, getRelativePath(objectPath))).iterator();
        } else if (oidFile != null) {
            return oidFileIterator();
        } else if (query != null && dataSource != null) {
            return sqlQueryIterator();
        } else if (nameFile != null) {
            return nameFileIterator();
        } else {
            throw new IllegalStateException("One of namespaceRoot, oidFile, query or nameFile must be set");
        }
    }

    @Override
    public Iterator<AtmosSyncObject> childIterator(AtmosSyncObject syncObject) {
        if (syncObject.isDirectory())
            return new AtmosDirectoryIterator((ObjectPath) syncObject.getRawSourceIdentifier(),
                    syncObject.getRelativePath());
        else
            return null;
    }

    @Override
    public void delete(final AtmosSyncObject syncObject) {
        if (deleteTags) {
            // get all tags for the object
            Map<String, Boolean> tags = time(new Function<Map<String, Boolean>>() {
                @Override
                public Map<String, Boolean> call() {
                    return atmos.getUserMetadataNames(syncObject.getRawSourceIdentifier());
                }
            }, OPERATION_GET_USER_META);
            for (final String name : tags.keySet()) {
                // if a tag is listable, delete it
                if (tags.get(name)) time(new Function<Void>() {
                    @Override
                    public Void call() {
                        atmos.deleteUserMetadata(syncObject.getRawSourceIdentifier(), name);
                        return null;
                    }
                }, OPERATION_DELETE_USER_META);
            }
        }
        try {
            // delete the object
            time(new Function<Void>() {
                @Override
                public Void call() {
                    atmos.delete(syncObject.getRawSourceIdentifier());
                    return null;
                }
            }, OPERATION_DELETE_OBJECT);
        } catch (AtmosException e) {
            if (e.getErrorCode() == 1023)
                log.warn("could not delete non-empty directory {}", syncObject.getRawSourceIdentifier());
            else throw e;
        }
    }

    private Iterator<AtmosSyncObject> sqlQueryIterator() {
        return new ReadOnlyIterator<AtmosSyncObject>() {
            private Connection con;
            private PreparedStatement stmt;
            private ResultSet rs;

            @Override
            protected AtmosSyncObject getNextObject() {
                try {
                    if (rs == null) {
                        con = dataSource.getConnection();
                        stmt = con.prepareStatement(query);
                        rs = stmt.executeQuery();
                    }

                    if (rs.next()) {
                        ObjectId objectId = new ObjectId(rs.getString(1));
                        return new AtmosSyncObject(AtmosSource.this, atmos, objectId, getRelativePath(objectId));
                    } else {
                        log.info("Reached end of query");
                        try {
                            rs.close();
                            stmt.close();
                            con.close();
                        } catch (SQLException e) {
                            log.warn("Error closing resources: " + e, e);
                        }
                        return null;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Error while querying the database", e);
                }
            }
        };
    }

    private Iterator<AtmosSyncObject> oidFileIterator() {
        final Iterator<String> fileIterator = new FileLineIterator(oidFile);

        return new ReadOnlyIterator<AtmosSyncObject>() {
            @Override
            protected AtmosSyncObject getNextObject() {
                if (fileIterator.hasNext()) {
                    ObjectId objectId = new ObjectId(fileIterator.next());
                    return new AtmosSyncObject(AtmosSource.this, atmos, objectId, getRelativePath(objectId));
                }
                return null;
            }
        };
    }

    private Iterator<AtmosSyncObject> nameFileIterator() {
        final Iterator<String> fileIterator = new FileLineIterator(nameFile);

        return new ReadOnlyIterator<AtmosSyncObject>() {
            @Override
            protected AtmosSyncObject getNextObject() {
                if (fileIterator.hasNext()) {
                    ObjectPath objectPath = new ObjectPath(fileIterator.next().trim());
                    return new AtmosSyncObject(AtmosSource.this, atmos, objectPath, getRelativePath(objectPath));
                }
                return null;
            }
        };
    }

    private String getRelativePath(ObjectIdentifier identifier) {
        if (identifier instanceof ObjectPath) {
            String path = identifier.toString();

            if (namespaceRoot != null && path.startsWith(namespaceRoot))
                path = path.substring(namespaceRoot.length());

            if (path.startsWith("/")) path = path.substring(1);

            return path;

        } else {
            return identifier.toString();
        }
    }

    protected class AtmosDirectoryIterator extends ReadOnlyIterator<AtmosSyncObject> {
        private ObjectPath directory;
        private String relativePath;
        private ListDirectoryRequest listRequest;
        private Iterator<DirectoryEntry> atmosIterator;

        public AtmosDirectoryIterator(ObjectPath directory, String relativePath) {
            this.directory = directory;
            this.relativePath = relativePath;
            listRequest = new ListDirectoryRequest().path(directory).includeMetadata(true);
        }

        @Override
        protected AtmosSyncObject getNextObject() {
            if (getAtmosIterator().hasNext()) {
                DirectoryEntry entry = getAtmosIterator().next();
                ObjectPath objectPath = new ObjectPath(directory, entry);
                String childPath = relativePath + "/" + entry.getFilename();
                childPath = childPath.replaceFirst("^/", "");
                Long size = null;
                if (entry.getSystemMetadataMap() != null && entry.getSystemMetadataMap().get("size") != null)
                    size = Long.parseLong(entry.getSystemMetadataMap().get("size").getValue());
                return new AtmosSyncObject(AtmosSource.this, atmos, objectPath, childPath, size);
            }
            return null;
        }

        private synchronized Iterator<DirectoryEntry> getAtmosIterator() {
            if (atmosIterator == null || (!atmosIterator.hasNext() && listRequest.getToken() != null)) {
                atmosIterator = getNextBlock().iterator();
            }
            return atmosIterator;
        }

        private List<DirectoryEntry> getNextBlock() {
            return time(new Function<List<DirectoryEntry>>() {
                @Override
                public List<DirectoryEntry> call() {
                    return atmos.listDirectory(listRequest).getEntries();
                }
            }, OPERATION_LIST_DIRECTORY);
        }
    }

    public List<URI> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(List<URI> endpoints) {
        this.endpoints = endpoints;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public AtmosApi getAtmos() {
        return atmos;
    }

    public void setAtmos(AtmosApi atmos) {
        this.atmos = atmos;
    }

    /**
     * @return the namespaceRoot
     */
    public String getNamespaceRoot() {
        return namespaceRoot;
    }

    /**
     * @param namespaceRoot the namespaceRoot to set
     */
    public void setNamespaceRoot(String namespaceRoot) {
        this.namespaceRoot = namespaceRoot;
    }

    /**
     * @return the oidFile
     */
    public String getOidFile() {
        return oidFile;
    }

    /**
     * @param oidFile the oidFile to set
     */
    public void setOidFile(String oidFile) {
        this.oidFile = oidFile;
    }

    public String getNameFile() {
        return nameFile;
    }

    public void setNameFile(String nameFile) {
        this.nameFile = nameFile;
    }

    /**
     * @return the query
     */
    public String getQuery() {
        return query;
    }

    /**
     * @param query the query to set
     */
    public void setQuery(String query) {
        this.query = query;
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

    public boolean isDeleteTags() {
        return deleteTags;
    }

    public void setDeleteTags(boolean deleteTags) {
        this.deleteTags = deleteTags;
    }
}
