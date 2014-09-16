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

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.*;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.ListDirectoryRequest;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.*;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads objects from an Atmos system.
 */
public class AtmosSource extends SyncSource<AtmosSource.AtmosSyncObject> {
    private static final Logger l4j = Logger.getLogger(AtmosSource.class);

    /**
     * This pattern is used to activate this plugin.
     */
    public static final String SOURCE_PATTERN = "^atmos:(http|https)://([a-zA-Z0-9/\\-]+):([a-zA-Z0-9\\+/=]+)@([^/]*?)(:[0-9]+)?(?:/)?$";

    public static final String SOURCE_NAMESPACE_OPTION = "source-namespace";
    public static final String SOURCE_NAMESPACE_DESC = "The source within the Atmos namespace.  Note that a directory must end with a trailing slash (e.g. /dir1/dir2/) otherwise it will be interpreted as a single file.  Not compatible with source-oid-list.";
    public static final String SOURCE_NAMESPACE_ARG_NAME = "atmos-path";

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
    private static final String OPERATION_LIST_DIRECTORY = "AtmosListDirectory";
    private static final String OPERATION_GET_ALL_META = "AtmosGetAllMeta";
    private static final String OPERATION_GET_OBJECT_INFO = "AtmosGetObjectInfo";
    private static final String OPERATION_GET_OBJECT_STREAM = "AtmosGetObjectStream";

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
        opts.addOption(new OptionBuilder().withLongOpt(SOURCE_NAMESPACE_OPTION).withDescription(SOURCE_NAMESPACE_DESC)
                .hasArg().withArgName(SOURCE_NAMESPACE_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(SOURCE_OIDLIST_OPTION).withDescription(SOURCE_OIDLIST_DESC)
                .hasArg().withArgName(SOURCE_OIDLIST_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(SOURCE_NAMELIST_OPTION).withDescription(SOURCE_NAMELIST_DESC)
                .hasArg().withArgName(SOURCE_NAMELIST_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(SOURCE_SQLQUERY_OPTION).withDescription(SOURCE_SQLQUERY_DESC)
                .hasArg().withArgName(SOURCE_SQLQUERY_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_URL_OPT).withDescription(JDBC_URL_DESC)
                .hasArg().withArgName(JDBC_URL_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_DRIVER_OPT).withDescription(JDBC_DRIVER_DESC)
                .hasArg().withArgName(JDBC_DRIVER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_USER_OPT).withDescription(JDBC_USER_DESC)
                .hasArg().withArgName(JDBC_USER_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(JDBC_PASSWORD_OPT).withDescription(JDBC_PASSWORD_DESC)
                .hasArg().withArgName(JDBC_PASSWORD_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DELETE_TAGS_OPT).withDescription(DELETE_TAGS_DESC).create());
        return opts;
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.matches(SOURCE_PATTERN);
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        Pattern p = Pattern.compile(SOURCE_PATTERN);
        Matcher m = p.matcher(sourceUri);
        if (!m.matches()) {
            throw new ConfigurationException("source option does not match pattern (how did this plug-in get loaded?)");
        }
        String protocol = m.group(1);
        uid = m.group(2);
        secret = m.group(3);
        String[] hosts = m.group(4).split(",");
        int port = -1;
        if (m.groupCount() == 5) {
            port = Integer.parseInt(m.group(5));
        }

        try {
            endpoints = new ArrayList<>();
            for (String host : hosts) {
                endpoints.add(new URI(protocol, null, host, port, null, null, null));
            }
        } catch (URISyntaxException e) {
            throw new ConfigurationException("invalid endpoint URI", e);
        }

        if (line.hasOption(SOURCE_NAMESPACE_OPTION))
            namespaceRoot = line.getOptionValue(SOURCE_NAMESPACE_OPTION);

        if (line.hasOption(SOURCE_OIDLIST_OPTION)) {
            oidFile = line.getOptionValue(SOURCE_OIDLIST_OPTION);
            if (!"-".equals(oidFile)) {
                // Verify file
                File f = new File(oidFile);
                if (!f.exists()) {
                    throw new ConfigurationException(
                            MessageFormat.format(
                                    "The OID list file {0} does not exist",
                                    oidFile));
                }
            }
        }

        if (line.hasOption(SOURCE_NAMELIST_OPTION))
            nameFile = line.getOptionValue(SOURCE_NAMELIST_OPTION);

        if (line.hasOption(SOURCE_SQLQUERY_OPTION)) {
            query = line.getOptionValue(SOURCE_SQLQUERY_OPTION);

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
            setDataSource(cpds);
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        // No plugins currently incompatible with this one.
        if (atmos == null) {
            if (endpoints == null || uid == null || secret == null)
                throw new ConfigurationException("Must specify endpoints, uid and secret key");
            atmos = new AtmosApiClient(new AtmosConfig(uid, secret, endpoints.toArray(new URI[endpoints.size()])));
        }

        // Check authentication
        ServiceInformation info = atmos.getServiceInformation();
        LogMF.info(l4j, "Connected to Atmos {0} on {1}", info.getAtmosVersion(), endpoints);

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
                    "Only one of (--{0}, --{1}, --{2}, --{3}) is allowed",
                    SOURCE_NAMESPACE_OPTION, SOURCE_OIDLIST_OPTION,
                    SOURCE_NAMELIST_OPTION, SOURCE_SQLQUERY_OPTION));
        }
        if (optCount < 1) {
            throw new ConfigurationException(MessageFormat.format(
                    "One of (--{0}, --{1}, --{2}, --{3}) must be specified",
                    SOURCE_NAMESPACE_OPTION, SOURCE_OIDLIST_OPTION,
                    SOURCE_NAMELIST_OPTION, SOURCE_SQLQUERY_OPTION));
        }

        if (namespace) {
            if (!namespaceRoot.startsWith("/")) namespaceRoot = "/" + namespaceRoot;
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

    @Override
    public String getName() {
        return "Atmos Source";
    }

    @Override
    public String getDocumentation() {
        return "The Atmos source plugin is triggered by the source pattern:\n" +
                "atmos:http://uid:secret@host[:port]  or\n" +
                "atmos:https://uid:secret@host[:port]\n" +
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
            return Arrays.asList(new AtmosSyncObject(objectPath, getRelativePath(objectPath))).iterator();
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
        if (syncObject.hasChildren())
            return new AtmosDirectoryIterator((ObjectPath) syncObject.getRawSourceIdentifier(),
                    syncObject.getRelativePath());
        else
            return null;
    }

    @Override
    public void delete(AtmosSyncObject syncObject) {
        if (deleteTags) {
            List<String> tags = new ArrayList<>();
            for (Map.Entry<String, Boolean> entry :
                    atmos.getUserMetadataNames((ObjectIdentifier) syncObject.getRawSourceIdentifier()).entrySet()) {
                if (entry.getValue()) tags.add(entry.getKey());
            }
            atmos.deleteUserMetadata((ObjectIdentifier) syncObject.getRawSourceIdentifier(), tags.toArray(new String[tags.size()]));
        }
        try {
            atmos.delete((ObjectIdentifier) syncObject.getRawSourceIdentifier());
        } catch (AtmosException e) {
            if (e.getErrorCode() == 1023)
                LogMF.warn(l4j, "could not delete non-empty directory {0}", syncObject.getRawSourceIdentifier());
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
                        return new AtmosSyncObject(objectId, getRelativePath(objectId));
                    } else {
                        l4j.info("Reached end of query");
                        try {
                            rs.close();
                            stmt.close();
                            con.close();
                        } catch (SQLException e) {
                            l4j.warn("Error closing resources: " + e, e);
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
                    return new AtmosSyncObject(objectId, getRelativePath(objectId));
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
                    return new AtmosSyncObject(objectPath, getRelativePath(objectPath));
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

    /**
     * Encapsulates the information needed for reading from Atmos and does
     * some lazy loading of data.
     */
    public class AtmosSyncObject extends SyncObject<AtmosSyncObject> {
        private ObjectIdentifier sourceId;
        private boolean metaLoaded = false; // signifies user/system metadata only (not content-type, etc.)
        private long size = 0;

        public AtmosSyncObject(ObjectIdentifier sourceId, String relativePath) {
            super(sourceId.toString(), relativePath);
            this.sourceId = sourceId;
        }

        @Override
        public Object getRawSourceIdentifier() {
            return sourceId;
        }

        @Override
        public boolean hasData() {
            return !isDirectory();
        }

        @Override
        public long getSize() {
            loadMeta();
            return size;
        }

        @Override
        public InputStream createSourceInputStream() {
            if (isDirectory()) return null;
            return time(new Timeable<ReadObjectResponse<InputStream>>() {
                @Override
                public ReadObjectResponse<InputStream> call() {
                    return atmos.readObjectStream(sourceId, null);
                }
            }, OPERATION_GET_OBJECT_STREAM).getObject();
        }

        @Override
        public boolean hasChildren() {
            return isDirectory();
        }

        @Override
        public SyncMetadata getMetadata() {
            loadMeta();
            return super.getMetadata();
        }

        public boolean isDirectory() {
            return sourceId instanceof ObjectPath && ((ObjectPath) sourceId).isDirectory();
        }

        // HEAD object in Atmos
        private void loadMeta() {
            if (metaLoaded) return;
            synchronized (this) {
                if (metaLoaded) return;

                // deal with root of namespace
                if ("/".equals(sourceId.toString())) {
                    metadata = new AtmosMetadata();
                    metaLoaded = true;
                    return;
                }

                metadata = AtmosMetadata.fromObjectMetadata(time(new Timeable<ObjectMetadata>() {
                    @Override
                    public ObjectMetadata call() {
                        return atmos.getObjectMetadata(sourceId);
                    }
                }, OPERATION_GET_ALL_META));

                if (isDirectory()) {
                    size = 0;
                } else {
                    String sizeString = metadata.getSystemMetadataProp("size");
                    size = (sizeString == null) ? 0 : Long.parseLong(sizeString);

                    // GET ?info will give use retention/expiration
                    if (includeRetentionExpiration) {
                        ObjectInfo info = time(new Timeable<ObjectInfo>() {
                            @Override
                            public ObjectInfo call() {
                                return atmos.getObjectInfo(sourceId);
                            }
                        }, OPERATION_GET_OBJECT_INFO);
                        if (info.getRetention() != null) {
                            metadata.setRetentionEnabled(info.getRetention().isEnabled());
                            metadata.setRetentionEndDate(info.getRetention().getEndAt());
                        }
                        if (info.getExpiration() != null) {
                            metadata.setExpirationEnabled(info.getExpiration().isEnabled());
                            metadata.setExpirationDate(info.getExpiration().getEndAt());
                        }
                    }
                }

                metaLoaded = true;
            }
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
            listRequest = new ListDirectoryRequest().path(directory);
        }

        @Override
        protected AtmosSyncObject getNextObject() {
            if (getAtmosIterator().hasNext()) {
                DirectoryEntry entry = getAtmosIterator().next();
                ObjectPath objectPath = new ObjectPath(directory, entry);
                String childPath = relativePath + "/" + entry.getFilename();
                childPath = childPath.replaceFirst("^/", "");
                return new AtmosSyncObject(objectPath, childPath);
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
            return time(new Timeable<List<DirectoryEntry>>() {
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
