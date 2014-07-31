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
package com.emc.atmos.sync.plugins;

import com.emc.atmos.api.*;
import com.emc.atmos.api.bean.*;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.ListDirectoryRequest;
import com.emc.atmos.sync.Timeable;
import com.emc.atmos.sync.util.AtmosMetadata;
import com.emc.atmos.sync.util.CountingInputStream;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads objects from an Atmos system.
 * @author cwikj
 */
public class AtmosSource extends MultithreadedCrawlSource implements InitializingBean {
	private static final Logger l4j = Logger.getLogger(AtmosSource.class);
	
	/**
	 * This pattern is used to activate this plugin.
	 */
	public static final String URI_PATTERN = "^(http|https)://([a-zA-Z0-9/\\-]+):([a-zA-Z0-9\\+/=]+)@([^/]*?)(:[0-9]+)?(?:/)?$";
	
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
    private static final String OPERATION_READ_OBJECT_STREAM = "AtmosReadObjectStream";

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

    private boolean includeRetentionExpiration;

    private int bufferSize = CommonOptions.DEFAULT_BUFFER_SIZE;

	public AtmosSource() {
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		
		opts.addOption(OptionBuilder.withDescription(SOURCE_NAMESPACE_DESC)
				.withLongOpt(SOURCE_NAMESPACE_OPTION)
				.hasArg().withArgName(SOURCE_NAMESPACE_ARG_NAME).create());
		
		opts.addOption(OptionBuilder.withDescription(SOURCE_OIDLIST_DESC)
				.withLongOpt(SOURCE_OIDLIST_OPTION)
				.hasArg().withArgName(SOURCE_OIDLIST_ARG_NAME).create());
		
		opts.addOption(OptionBuilder.withDescription(SOURCE_NAMELIST_DESC)
				.withLongOpt(SOURCE_NAMELIST_OPTION)
				.hasArg().withArgName(SOURCE_NAMELIST_ARG_NAME).create());

        opts.addOption(OptionBuilder.withDescription(SOURCE_SQLQUERY_DESC)
                .withLongOpt(SOURCE_SQLQUERY_OPTION)
                .hasArg().withArgName(SOURCE_SQLQUERY_ARG_NAME).create());

        opts.addOption(OptionBuilder.withDescription(JDBC_URL_DESC)
                .withLongOpt(JDBC_URL_OPT)
                .hasArg().withArgName(JDBC_URL_ARG_NAME).create());

        opts.addOption(OptionBuilder.withDescription(JDBC_DRIVER_DESC)
                .withLongOpt(JDBC_DRIVER_OPT)
                .hasArg().withArgName(JDBC_DRIVER_ARG_NAME).create());

        opts.addOption(OptionBuilder.withDescription(JDBC_USER_DESC)
                .withLongOpt(JDBC_USER_OPT)
                .hasArg().withArgName(JDBC_USER_ARG_NAME).create());

        opts.addOption(OptionBuilder.withDescription(JDBC_PASSWORD_DESC)
                .withLongOpt(JDBC_PASSWORD_OPT)
                .hasArg().withArgName(JDBC_PASSWORD_ARG_NAME).create());
        // Add parent options
		addOptions( opts );
		
		return opts;
	}

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		if(line.hasOption(CommonOptions.SOURCE_OPTION)) {
			Pattern p = Pattern.compile(URI_PATTERN);
			String source = line.getOptionValue(CommonOptions.SOURCE_OPTION);
			Matcher m = p.matcher(source);
			if(!m.matches()) {
				LogMF.debug(l4j, "{0} does not match {1}", source, p);
				return false;
			}
			protocol = m.group(1);
			uid = m.group(2);
			secret = m.group(3);
			String sHost = m.group(4);
			String sPort = null;
			if(m.groupCount() == 5) {
				sPort = m.group(5);
			}
			hosts = Arrays.asList(sHost.split(","));
			if(sPort != null) {
				port = Integer.parseInt(sPort.substring(1));
			} else {
				if("https".equals(protocol)) {
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
			
			if(optCount > 1) {
				throw new IllegalArgumentException(MessageFormat.format(
						"Only one of (--{0}, --{1}, --{2}, --{3}) is allowed",
						SOURCE_NAMESPACE_OPTION, SOURCE_OIDLIST_OPTION,
						SOURCE_NAMELIST_OPTION, SOURCE_SQLQUERY_OPTION));
			}
			if(optCount < 1) {
				throw new IllegalArgumentException(MessageFormat.format(
						"One of (--{0}, --{1}, --{2}, --{3}) must be specified",
						SOURCE_NAMESPACE_OPTION, SOURCE_OIDLIST_OPTION,
						SOURCE_NAMELIST_OPTION, SOURCE_SQLQUERY_OPTION));
			}
			
			if(namespace) {
				namespaceRoot = line.getOptionValue(SOURCE_NAMESPACE_OPTION);
			}
			if(objectlist) {
				oidFile = line.getOptionValue(SOURCE_OIDLIST_OPTION);
				if("-".equals(oidFile)) {
					
				} else {
					// Verify file
					File f = new File(oidFile);
					if(!f.exists()) {
						throw new IllegalArgumentException(
								MessageFormat.format(
										"The OID list file {0} does not exist", 
										oidFile));
					}
				}
			}
			if(namelist) {
				nameFile = line.getOptionValue(SOURCE_NAMELIST_OPTION);
			}
            if(sqllist) {
                query = line.getOptionValue(SOURCE_SQLQUERY_OPTION);

                // Initialize a c3p0 pool
                ComboPooledDataSource cpds = new ComboPooledDataSource();
                try {
                    cpds.setDriverClass( line.getOptionValue(JDBC_DRIVER_OPT) );
                    cpds.setJdbcUrl( line.getOptionValue(JDBC_URL_OPT) );
                    cpds.setUser( line.getOptionValue(JDBC_USER_OPT) );
                    cpds.setPassword( line.getOptionValue(JDBC_PASSWORD_OPT) );
                } catch (PropertyVetoException e) {
                    throw new RuntimeException("Unable to initialize JDBC driver: " + e.getMessage(), e);
                }
                cpds.setMaxStatements(180);
                setDataSource(cpds);
            }

            includeRetentionExpiration = line.hasOption(CommonOptions.INCLUDE_RETENTION_EXPIRATION_OPTION);

            if (line.hasOption(CommonOptions.IO_BUFFER_SIZE_OPTION)) {
                bufferSize = Integer.parseInt(line.getOptionValue(CommonOptions.IO_BUFFER_SIZE_OPTION));
            }

            // Parse threading options
			super.parseOptions(line);
			
			return true;
		}
		
		return false;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notEmpty(hosts);
		Assert.hasText(protocol);
		Assert.hasText(secret);
		Assert.hasText(uid);
	}


	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
		// No plugins currently incompatible with this one.
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "Atmos Source";
	}

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "The Atmos source plugin is triggered by the source pattern:\n" +
				"http://uid:secret@host[:port]  or\n" +
				"https://uid:secret@host[:port]\n" +
				"Note that the uid should be the 'full token ID' including the " +
				"subtenant ID and the uid concatenated by a slash\n" +
				"If you want to software load balance across multiple hosts, " +
				"you can provide a comma-delimited list of hostnames or IPs " +
				"in the host part of the URI.\n" +
				"This source plugin is multithreaded and you should use the " +
				"--source-threads option to specify how many threads to use." +
				" The default thread count is one.";
	}
	
	public void run() {
		running = true;
		
		if(atmos == null) {
			atmos = new AtmosApiClient(new AtmosConfig(uid, secret, getEndpoints()));
		}
		
		// Check authentication
		ServiceInformation info = atmos.getServiceInformation();
		LogMF.info(l4j, "Connected to Atmos {0} on {1}", info.getAtmosVersion(),
				hosts);
		
		// Check to see if UTF-8 is supported.
		if(!info.hasFeature(ServiceInformation.Feature.Utf8)) {
			l4j.warn( "Unicode not supported" );
		}
		
		if(namespaceRoot != null) {
			readNamespace();
		} else if(oidFile != null) {
			readOIDs();
		} else if(query != null && dataSource != null) {
			readSqlQuery();
		} else if(nameFile != null) {
			readNames();
		} else {
			throw new IllegalStateException("One of namespaceRoot, oidFile, or query must be set");
		}
	}

	private void readSqlQuery() {
		initQueue();
		
		Connection con;
		PreparedStatement stmt;
		ResultSet rs;
		try {
			con = dataSource.getConnection();
			stmt = con.prepareStatement(query);
			rs = stmt.executeQuery();
		} catch(SQLException e) {
			throw new RuntimeException("Error querying for ids: " + e, e);
		}
		
		while(running) {
			try {
				if(!rs.next()) {
					l4j.info("Reached end of input set");
					try {
						rs.close();
						stmt.close();
						con.close();
					} catch(SQLException e) {
						l4j.warn("Error closing connection: " + e, e);
					}
					break;
				}

				ObjectId oid = new ObjectId(rs.getString(1));
				ReadAtmosTask task = new ReadAtmosTask(oid);
				submitTransferTask(task);

			} catch (SQLException e) {
				throw new RuntimeException("Error fetching rows from DB: " + e, e);
			}			
			
		}
		
		// We've read all of our data; go into the normal loop now and
		// run until we're out of tasks.
		runQueue();
	}
	
	private void readOIDs() {
		initQueue();
		
		BufferedReader br;
		try {
			if("-".equals(oidFile)) {
				br = new BufferedReader(new InputStreamReader(System.in));
			} else {
				br = new BufferedReader(new FileReader(new File(oidFile)));
			}
			
			while(running) {				
				String line = br.readLine();
				if(line == null) {
					break;
				}
				ObjectId id = new ObjectId(line.trim());
				ReadAtmosTask task = new ReadAtmosTask(id);
				submitTransferTask(task);
				
			}
			
			// We've read all of our data; go into the normal loop now and
			// run until we're out of tasks.
			runQueue();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(5);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(6);
		}
		
	}
	
	private void readNames() {
		initQueue();
		
		BufferedReader br;
		try {
			if("-".equals(nameFile)) {
				br = new BufferedReader(new InputStreamReader(System.in));
			} else {
				br = new BufferedReader(new FileReader(new File(nameFile)));
			}
			
			while(running) {
				String line = br.readLine();
				if(line == null) {
					break;
				}
				ObjectPath op = new ObjectPath(line.trim());
				ReadAtmosTask task = new ReadAtmosTask(op);
				if(op.isDirectory()) {
					submitCrawlTask(task);
				} else {
					submitTransferTask(task);
				}
				
			}
			
			// We've read all of our data; go into the normal loop now and
			// run until we're out of tasks.
			runQueue();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(5);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(6);
		}
		
	}


	private void readNamespace() {
		initQueue();
		
		// Enqueue the root task.
		ReadAtmosTask rootTask = new ReadAtmosTask(new ObjectPath(namespaceRoot));
		submitCrawlTask(rootTask);
		
		runQueue();
	}
	
	private URI[] getEndpoints() {
        try {
            List<URI> uris = new ArrayList<URI>();
            for (String host : hosts) {
                uris.add(new URI(protocol, null, host, port, null, null, null));
            }
            return uris.toArray(new URI[hosts.size()]);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to create endpoints", e);
        }
    }
	

	@Override
	public void terminate() {
		running = false;
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
	 * This is the core task node that executes inside the thread pool.  It
	 * handles the dependencies when enumerating directories.
	 */
	class ReadAtmosTask implements Runnable {
		private ObjectIdentifier id;

		public ReadAtmosTask(ObjectIdentifier id) {
			this.id = id;
		}

		@Override
		public void run() {
			AtmosSyncObject obj;
			try {
				obj = new AtmosSyncObject(id);
			} catch (URISyntaxException e) {
				l4j.error("Could not initialize AtmosSyncObject: " + e, e);
				return;
			}
			try {
				getNext().filter(obj);
				complete(obj);
			} catch(Throwable t) {
				failed(obj, t);
				return;
			}

			if(id instanceof ObjectPath) {
				ObjectPath op = (ObjectPath)id;
				if(op.toString().equals("/apache/")) {
					l4j.debug("Skipping " + op);
					return;
				}
				if(op.isDirectory()) {
					try {
						listDirectory(op);
					} catch(Exception e) {
						l4j.error("Failed to list directory " + op + ": " + e, e);
						failed(obj, e);
					}
				}
			}
		}

		/**
		 * Lists the namespace directory and enqueues task nodes for each
		 * child.
		 */
		private void listDirectory(final ObjectPath op) {

			final ListDirectoryRequest request = new ListDirectoryRequest().path(op);
			List<DirectoryEntry> ents;
			
			l4j.debug(">>Start listing " + op);
			do {
			    ents = time(new Timeable<List<DirectoryEntry>>() {
                        @Override
                        public List<DirectoryEntry> call() {
                            return atmos.listDirectory(request).getEntries();
                        }
                    }, OPERATION_LIST_DIRECTORY);
				for(DirectoryEntry ent : ents) {
					// Create a child task for each child entry in the directory
					// and enqueue it in the graph.
                    ObjectPath entryPath = new ObjectPath(op, ent);
					ReadAtmosTask child = new ReadAtmosTask(entryPath);
					if(ent.getFileType() == DirectoryEntry.FileType.directory) {
						LogMF.debug(l4j, "+crawl: {0}", entryPath);
						submitCrawlTask(child);
					} else {
						LogMF.debug(l4j, "+transfer: {0}", entryPath);
						submitTransferTask(child);
					}
				}
			} while(request.getToken() != null);
			l4j.debug("<<Done listing " + op);
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "ReadAtmosTask [id=" + id + "]";
		}
		
		
		
	}
	
	/**
	 * Encapsulates the information needed for reading from Atmos and does
	 * some lazy loading of data.
	 */
	class AtmosSyncObject extends SyncObject {
		private ObjectIdentifier sourceId;
		private CountingInputStream in;
		private String relativePath;
		private boolean metadataLoaded;

		public AtmosSyncObject(ObjectIdentifier sourceId) throws URISyntaxException {
			this.sourceId = sourceId;
			SourceAtmosId ann = new SourceAtmosId();
			if(sourceId instanceof ObjectPath) {
				setSourceURI(new URI(protocol, uid + ":" + secret, hosts.get(0), 
						port, sourceId.toString(), null, null));
				if(((ObjectPath)sourceId).isDirectory()) {
					setDirectory(true);
				}
				ann.setPath((ObjectPath) sourceId);
				
				String sourcePath = sourceId.toString();
				if(namespaceRoot != null) {
					// Subtract the relative path
					if(sourcePath.startsWith(namespaceRoot)) {
						sourcePath = sourcePath
								.substring(namespaceRoot.length());
					}
				}
				if(sourcePath.startsWith("/")) {
					sourcePath = sourcePath.substring(1);
				}
				relativePath = sourcePath;
				
			} else {
				setSourceURI(new URI(protocol, uid + ":" + secret, 
						hosts.get(0), port, "/" + sourceId.toString(), null, null));	
				ann.setId((ObjectId) sourceId);
				relativePath = sourceId.toString();
			}
			metadataLoaded = false;
			this.addAnnotation(ann);
		}

		@Override
		public InputStream getInputStream() {
			if(in == null) {
				if(sourceId instanceof ObjectPath 
						&& ((ObjectPath)sourceId).isDirectory()) {
					// Directories don't have content.
					in = new CountingInputStream(
							new ByteArrayInputStream(new byte[0]));
					setSize(0);
				} else {
					try {
						in = time(new Timeable<CountingInputStream>() {
                            @Override
                            public CountingInputStream call() {
                                return new CountingInputStream(
                                        new BufferedInputStream(atmos.readObjectStream(sourceId, null).getObject(), bufferSize));
                            }
                        }, OPERATION_READ_OBJECT_STREAM);
					} catch(Exception e) {
						throw new RuntimeException("Failed to get input " +
								"stream for " + sourceId + ": " + 
								e.getMessage(), e);
					}
				}
			}
			return in;
		}
		
		public long getBytesRead() {
			if(in != null) {
				return in.getBytesRead();
			} else {
				return 0;
			}
		}
		
		@Override
		public String toString() {
			return "AtmosSyncObject: " + sourceId;
		}
				
		@Override
		public long getSize() {
			if(!metadataLoaded) {
				getMeta();
			}
			return super.getSize();
		}
		
		@Override
		public AtmosMetadata getMetadata() {
			if(!metadataLoaded) {
				getMeta();
			}
			return super.getMetadata();
		}

		/**
		 * Query Atmos for the object's metadata.
		 */
		private void getMeta() {
			if(sourceId instanceof ObjectPath && ((ObjectPath)sourceId).isDirectory()) {
				AtmosMetadata am = new AtmosMetadata();
				setSize(0);
				am.setContentType(null);
				am.setMetadata(time(new Timeable<Map<String, Metadata>>() {
                    @Override
                    public Map<String, Metadata> call() {
                        return atmos.getUserMetadata(sourceId);
                    }
                }, OPERATION_GET_USER_META));
				am.setSystemMetadata(time(new Timeable<Map<String, Metadata>>() {
                    @Override
                    public Map<String, Metadata> call() {
                        return atmos.getSystemMetadata(sourceId);
                    }
                }, OPERATION_GET_SYSTEM_META));
				setMetadata(am);
			} else {
				ObjectMetadata meta = time(new Timeable<ObjectMetadata>() {
                    @Override
                    public ObjectMetadata call() {
                        return atmos.getObjectMetadata(sourceId);
                    }
                }, OPERATION_GET_ALL_META);
				AtmosMetadata am = AtmosMetadata.fromObjectMetadata(meta);
				setMetadata(am);
				if(am.getSystemMetadata().get("size") != null) {
					setSize(Long.parseLong(am.getSystemMetadata().get("size").getValue()));
				}
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
			metadataLoaded = true;
		}

		@Override
		public String getRelativePath() {
			return relativePath;
		}
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

    public boolean isIncludeRetentionExpiration() {
        return includeRetentionExpiration;
    }

    public void setIncludeRetentionExpiration(boolean includeRetentionExpiration) {
        this.includeRetentionExpiration = includeRetentionExpiration;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
