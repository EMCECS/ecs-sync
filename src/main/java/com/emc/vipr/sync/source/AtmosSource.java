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
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
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

    // timed operations
    private static final String OPERATION_LIST_DIRECTORY = "AtmosListDirectory";
    private static final String OPERATION_GET_USER_META = "AtmosGetUserMeta";
    private static final String OPERATION_GET_SYSTEM_META = "AtmosGetSystemMeta";
    private static final String OPERATION_GET_ALL_META = "AtmosGetAllMeta";
    private static final String OPERATION_GET_OBJECT_INFO = "AtmosGetObjectInfo";
    private static final String OPERATION_GET_OBJECT_STREAM = "AtmosGetObjectStream";

    private List<String> hosts;
    private String protocol;
    private int port;
    private String uid;
    private String secret;
    private AtmosApi atmos;
    private DataSource dataSource;

    private String namespaceRoot;
    private String oidFile;
    private String query;
    private String nameFile;

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
        protocol = m.group(1);
        uid = m.group(2);
        secret = m.group(3);
        String sHost = m.group(4);
        String sPort = null;
        if (m.groupCount() == 5) {
            sPort = m.group(5);
        }
        hosts = Arrays.asList(sHost.split(","));
        if (sPort != null) {
            port = Integer.parseInt(sPort.substring(1));
        } else {
            if ("https".equals(protocol)) {
                port = 443;
            } else {
                port = 80;
            }
        }

        boolean namespace = line.hasOption(SOURCE_NAMESPACE_OPTION);
        boolean objectlist = line.hasOption(SOURCE_OIDLIST_OPTION);
        boolean namelist = line.hasOption(SOURCE_NAMELIST_OPTION);
        boolean sqllist = line.hasOption(SOURCE_SQLQUERY_OPTION);

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
            namespaceRoot = line.getOptionValue(SOURCE_NAMESPACE_OPTION);
        }
        if (objectlist) {
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
        if (namelist) {
            nameFile = line.getOptionValue(SOURCE_NAMELIST_OPTION);
        }
        if (sqllist) {
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
    public void validateChain(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        // No plugins currently incompatible with this one.
        Assert.notEmpty(hosts);
        Assert.hasText(protocol);
        Assert.hasText(secret);
        Assert.hasText(uid);
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
        if (atmos == null) {
            atmos = new AtmosApiClient(new AtmosConfig(uid, secret, getEndpoints()));
        }

        // Check authentication
        ServiceInformation info = atmos.getServiceInformation();
        LogMF.info(l4j, "Connected to Atmos {0} on {1}", info.getAtmosVersion(), hosts);

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
    public void delete(AtmosSyncObject syncObject) {
        atmos.delete((ObjectIdentifier) syncObject.getRawSourceIdentifier());
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

    private URI[] getEndpoints() {
        try {
            List<URI> uris = new ArrayList<>();
            for (String host : hosts) {
                uris.add(new URI(protocol, null, host, port, null, null, null));
            }
            return uris.toArray(new URI[hosts.size()]);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to create endpoints", e);
        }
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
        private InputStream in = null;
        private boolean objectLoaded = false;
        private boolean metaLoaded = false; // signifies user/system metadata only (not content-type, etc.)
        private boolean directory;
        private long size = 0;

        public AtmosSyncObject(ObjectIdentifier sourceId, String relativePath,
                               Map<String, Metadata> userMetadata, Map<String, Metadata> systemMetadata) {
            this(sourceId, relativePath);

            AtmosMetadata am = new AtmosMetadata();
            am.setMetadata(userMetadata);
            am.setSystemMetadata(systemMetadata);
            super.setMetadata(am);

            metaLoaded = true;
        }

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
            loadObject();
            return size;
        }

        @Override
        public InputStream createSourceInputStream() {
            loadObject();
            return in;
        }

        @Override
        public boolean hasChildren() {
            loadObject();
            return isDirectory();
        }

        @Override
        public Iterator<AtmosSyncObject> childIterator() {
            if (sourceId instanceof ObjectPath)
                return new AtmosDirectoryIterator((ObjectPath) sourceId, relativePath);
            else
                return null;
        }

        @Override
        public SyncMetadata getMetadata() {
            loadObject();
            return super.getMetadata();
        }

        public boolean isDirectory() {
            loadObject();
            return directory;
        }

        // load the object from Atmos
        private void loadObject() {
            if (objectLoaded) return;
            synchronized (this) {
                if (objectLoaded) return;

                in = new ByteArrayInputStream(new byte[]{});

                AtmosMetadata am = new AtmosMetadata();

                // deal with root of namespace
                if ("/".equals(sourceId.toString())) {
                    metadata = am;
                    objectLoaded = true;
                    return;
                }

                // first figure out if this is a directory
                directory = false;
                boolean sysMetaLoaded = false;
                if (sourceId instanceof ObjectPath) {
                    if (((ObjectPath) sourceId).isDirectory()) {
                        // can infer if path ends in slash
                        directory = true;
                    } else {
                        // otherwise, pull system meta and get the type
                        if (!metaLoaded) {
                            am.setSystemMetadata(time(new Timeable<Map<String, Metadata>>() {
                                @Override
                                public Map<String, Metadata> call() {
                                    return atmos.getSystemMetadata(sourceId);
                                }
                            }, OPERATION_GET_SYSTEM_META));
                        }
                        sysMetaLoaded = true;
                        directory = "directory".equals(am.getSystemMetadataProp("type"));
                    }
                }

                if (directory) {
                    if (includeAcl) {
                        // must get ACL from HEAD
                        am = AtmosMetadata.fromObjectMetadata(time(new Timeable<ObjectMetadata>() {
                            @Override
                            public ObjectMetadata call() {
                                return atmos.getObjectMetadata(sourceId);
                            }
                        }, OPERATION_GET_ALL_META));
                    } else {
                        if (!metaLoaded) {
                            if (!sysMetaLoaded) {
                                am.setSystemMetadata(time(new Timeable<Map<String, Metadata>>() {
                                    @Override
                                    public Map<String, Metadata> call() {
                                        return atmos.getSystemMetadata(sourceId);
                                    }
                                }, OPERATION_GET_SYSTEM_META));
                            }
                            am.setMetadata(time(new Timeable<Map<String, Metadata>>() {
                                @Override
                                public Map<String, Metadata> call() {
                                    return atmos.getUserMetadata(sourceId);
                                }
                            }, OPERATION_GET_USER_META));
                        }
                    }
                } else {
                    if (metadataOnly) {
                        // must get content-type from HEAD
                        am = AtmosMetadata.fromObjectMetadata(time(new Timeable<ObjectMetadata>() {
                            @Override
                            public ObjectMetadata call() {
                                return atmos.getObjectMetadata(sourceId);
                            }
                        }, OPERATION_GET_ALL_META));
                    } else {
                        // just GET the whole object to make it a single round trip (meta, etc. is in the header)
                        ReadObjectResponse<InputStream> response = time(new Timeable<ReadObjectResponse<InputStream>>() {
                            @Override
                            public ReadObjectResponse<InputStream> call() {
                                return atmos.readObjectStream(sourceId, null);
                            }
                        }, OPERATION_GET_OBJECT_STREAM);
                        am = AtmosMetadata.fromObjectMetadata(response.getMetadata());
                        in = new BufferedInputStream(response.getObject(), bufferSize);
                    }
                    String sizeString = am.getSystemMetadataProp("size");
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
                            am.setRetentionEnabled(info.getRetention().isEnabled());
                            am.setRetentionEndDate(info.getRetention().getEndAt());
                        }
                        if (info.getExpiration() != null) {
                            am.setExpirationEnabled(info.getExpiration().isEnabled());
                            am.setExpirationDate(info.getExpiration().getEndAt());
                        }
                    }
                }
                metadata = am;
                objectLoaded = true;
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
            listRequest = new ListDirectoryRequest().path(directory).includeMetadata(true);
        }

        @Override
        protected AtmosSyncObject getNextObject() {
            if (getAtmosIterator().hasNext()) {
                DirectoryEntry entry = getAtmosIterator().next();
                ObjectPath objectPath = new ObjectPath(directory, entry);
                return new AtmosSyncObject(objectPath, relativePath + "/" + directory.getFilename(),
                        entry.getUserMetadataMap(), entry.getSystemMetadataMap());
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

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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
}
