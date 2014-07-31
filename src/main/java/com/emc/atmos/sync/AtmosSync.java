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
package com.emc.atmos.sync;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.activation.MimetypesFileTypeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import com.emc.esu.api.Acl;
import com.emc.esu.api.EsuApi;
import com.emc.esu.api.EsuException;
import com.emc.esu.api.Grant;
import com.emc.esu.api.Grantee;
import com.emc.esu.api.Grantee.GRANT_TYPE;
import com.emc.esu.api.Identifier;
import com.emc.esu.api.Metadata;
import com.emc.esu.api.MetadataList;
import com.emc.esu.api.ObjectPath;
import com.emc.esu.api.rest.LBEsuRestApiApache;

/**
 * Utility to recursively upload files to Atmos, similar to rsync.
 * @author cwikj
 */
public class AtmosSync {
	
	public static enum METADATA_MODE { BOTH, DIRECTORIES, FILES }
	public static enum MODE { DOWNLOAD_ONLY, FULL_SYNC, UPLOAD_ONLY }
	private static final Logger l4j = Logger.getLogger(AtmosSync.class);
	public static final String MTIME_NAME = "atmossync_mtime";
	public static final String META_DIR = ".atmosmeta";

	
	private Acl acl;
	private long byteCount;
	private int completedCount = 0;
	private boolean delete;
	private EsuApi esu;
	private int failedCount = 0;
	private Set<TaskNode> failedItems;
	private int fileCount;
	private boolean force;
	private SimpleDirectedGraph<TaskNode, DefaultEdge> graph;
	private String[] hosts;
	private File localroot;
	private MetadataList meta;
	private METADATA_MODE metadataMode;
	private MimetypesFileTypeMap mimeMap;
	private ThreadPoolExecutor pool;
	private int port;
	private String proxyHost;
	private boolean proxyHttps;
	private String proxyPassword;
	private int proxyPort;
	private String proxyUser;
	private LinkedBlockingQueue<Runnable> queue;
	private String remoteroot;
	private String secret;
	private MODE syncMode;
	private int threads;
	private String uid;
	private boolean syncingMetadata;
	


	/**
	 * Encode characters that Atmos doesn't support (specifically @ and ?)
	 * @param op the path
	 * @return the path with invalid characters replaced
	 */
	public static String encodeObjectPath( String op ) {
		op = op.replace( "?", "." );
		op = op.replace( "@", "." );
		return op;
	}

	public static ObjectPath getParentDir(ObjectPath op) {
		String ops = op.toString();
		if( ops.endsWith( "/" ) ) {
			ops = ops.substring( 0, ops.length()-1 );
		}
		int lastslash = ops.lastIndexOf( '/' );
		if( lastslash == 0 ) {
			// root
			return null;
		}
		return new ObjectPath(ops.substring( 0, lastslash+1 ));
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Options options = new Options();
		
		Option o = new Option("u", "uid", true, "Atmos UID");
		o.setRequired(true);
		options.addOption(o);
		
		o = new Option( "m", "mode", true, "Sync mode: upload_only|download_only|full_sync" );
		o.setRequired(true);
		options.addOption(o);

		o = new Option("s", "secret", true, "Atmos Shared Secret");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("h", "host", true,
				"Atmos Access Point Host(s).  Use more than once to round-robin hosts.");
		o.setRequired(true);
		o.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(o);

		o = new Option("p", "port", true,
				"Atmos Access Point Port (Default 80)");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("r", "remoteroot", true, "Remote root path (e.g. \"/\")");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("l", "localroot", true, "Local root path.");
		o.setRequired(true);
		options.addOption(o);

		o = new Option("t", "threads", true, "Thread count.  Defaults to 8");
		o.setRequired(false);
		options.addOption(o);

		o = new Option("ua", "useracl", true,
				"User ACL (UID=READ|WRITE|FULL_CONTROL).  May be used more than once.");
		o.setRequired(false);
		o.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(o);

		o = new Option(
				"ga",
				"groupacl",
				true,
				"Group ACL (group=READ|WRITE|FULL_CONTROL).  May be used more than once.  Usually, group is 'other'");
		o.setRequired(false);
		o.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(o);
		
		o = new Option("D", "delete", false, "Delete local files after successful upload" );
		o.setRequired(false);
		options.addOption( o );
		
		o = new Option( "um", "usermeta", true, "User Metadata (name=value).  May be used more than once." );
		o.setRequired(false);
		o.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(o);
		
		o = new Option( "lm", "listablemeta", true, "Listable Metadata (name=value).  May be used more than once.  You may omit the = or =value for an empty tag value" );
		o.setRequired(false);
		o.setArgs(Option.UNLIMITED_VALUES);
		options.addOption(o);
		
		o = new Option( "F", "force", false, "Force upload even if files match.  Only applicable to upload_only or download_only modes." );
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option( "M", "metadatamode", true, "Metadata application mode (files|directories|both) defaults to both." );
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option( "MS", "metadatasync", false, "If specified, metadata will be synchronized as well.  On a filesystem, there will be a hidden directory called '.atmosmeta' with a file of the same name containing serialized JSON metadata and ACL information." );
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option( "ph", "proxyhost", true, "HTTP proxy host" );
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option( "pp", "proxyport", true, "HTTP proxy port (default: 8080)" );
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option( "pS", "proxyhttps", false, "If specified, HTTPS will " +
				"be used when connecting to the proxy server" );
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option( "pU", "proxyuser", true, "HTTP proxy username" );
		o.setRequired(false);
		options.addOption(o);
		
		o = new Option( "pP", "proxypassword", true, "HTTP proxy password" );
		o.setRequired(false);
		options.addOption(o);

		
		// create the parser
		CommandLineParser parser = new GnuParser();
		try {
			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			String uid = line.getOptionValue("uid");
			String secret = line.getOptionValue("secret");
			String[] host = line.getOptionValues("host");
			int port = Integer.parseInt(line.getOptionValue("port", "80"));
			String remoteroot = line.getOptionValue("remoteroot");
			String localroot = line.getOptionValue("localroot");
			int threads = Integer.parseInt(line.getOptionValue("threads", "8"));
			String[] useracl = line.getOptionValues("useracl");
			String[] groupacl = line.getOptionValues("groupacl");
			boolean delete = line.hasOption( "delete" );
			MODE mode = MODE.valueOf( line.getOptionValue("mode").toUpperCase() );
			boolean force = line.hasOption( "force" );
			String proxyHost = line.getOptionValue( "proxyhost" );
			int proxyPort = Integer.parseInt( line.getOptionValue( "proxyPort", "8080" ) );
			boolean proxyHttps = line.hasOption( "proxyhttps" );
			String proxyUser = line.getOptionValue( "proxyuser" );
			String proxyPassword = line.getOptionValue( "proxypassword" );
			
			String[] usermeta = line.getOptionValues("usermeta");
			String[] listablemeta = line.getOptionValues( "listablemeta" );
			MetadataList meta = new MetadataList();
			if( usermeta != null ) {
				parseMeta( usermeta, meta, false );
			}
			if( listablemeta != null ) {
				parseMeta( listablemeta, meta, true );
			}
			METADATA_MODE mmode = METADATA_MODE.valueOf( line.getOptionValue("metadatamode", "both").toUpperCase() );
			
			boolean metadatasync = line.hasOption("metadatasync");

			Acl acl = null;
			if( useracl != null || groupacl != null ) {
				acl = new Acl();
				if( useracl != null ) {
					parseAcl(useracl, acl, GRANT_TYPE.USER);
				}
				if( groupacl != null ) {
					parseAcl(groupacl, acl, GRANT_TYPE.GROUP);
				}
			}

			
			AtmosSync sync = new AtmosSync();
			
			sync.setUid( uid );
			sync.setSecret( secret );
			sync.setHosts( host );
			sync.setPort( port );
			sync.setRemoteRoot( remoteroot );
			sync.setLocalRoot( localroot );
			sync.setThreads( threads );
			sync.setAcl( acl );
			sync.setDelete( delete );
			sync.setSyncMode( mode );
			sync.setMeta( meta );
			sync.setForce( force );
			sync.setMetadataMode( mmode );
			sync.setProxyHost( proxyHost );
			sync.setProxyPort( proxyPort );
			sync.setProxyHttps( proxyHttps );
			sync.setProxyUsername( proxyUser );
			sync.setProxyPassword( proxyPassword );
			sync.setSyncingMetadata(metadatasync);
			
			sync.start();
		} catch (ParseException exp) {
			// oops, something went wrong
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar AtmosSync.jar", options);
		} catch (InterruptedException e) {
			l4j.error( "Execution interrupted " + e, e );
			System.exit( 4 );
		}
		System.exit(0);
	}
	private static void parseAcl(String[] aclstrs, Acl acl, GRANT_TYPE gtype ) {
		for( String str : aclstrs ) {
			String[] parts = str.split( "=", 2 );
			acl.addGrant( new Grant(new Grantee(parts[0], gtype), parts[1] ) );
		}
	}
	private static void parseMeta(String[] usermeta, MetadataList mlist,
			boolean listable) {
		for( String val : usermeta ) {
			String[] nvpair = val.split( "=", 2 );
			String name = nvpair[0];
			String value = nvpair.length>1?nvpair[1]:null;
			mlist.addMetadata( new Metadata( name, value, listable ) );
		}
	}

	
	public synchronized void failure(TaskNode task, File file, Identifier id,
			Exception e) {
		
		if( e instanceof EsuException ) {
			l4j.error( "Failed to sync " + file + " to " + id + ": " + e + " code: " + ((EsuException)e).getAtmosCode(), e );
			
		} else {
			l4j.error( "Failed to sync " + file + " to " + id + ": " + e, e );
		}
		failedCount++;
		failedItems.add( task );

	}
	public Acl getAcl() {
		return acl;
	}
	public EsuApi getEsu() {
		return esu;
	}
	public SimpleDirectedGraph<TaskNode, DefaultEdge> getGraph() {
		return graph;
	}
	
	public File getLocalroot() {
		return localroot;
	}
	public MetadataList getMeta() {
		return meta;
	}
	public METADATA_MODE getMetadataMode() {
		return metadataMode;
	}
	public MimetypesFileTypeMap getMimeMap() {
		return mimeMap;
	}
	public MODE getSyncMode() {
		return syncMode;
	}
	public void incrementFileCount() {
		fileCount++;
	}
	public boolean isDelete() {
		return delete;
	}

	public boolean isForce() {
		return force;
	}

	private void mkdirs(ObjectPath dir, Acl acl) {
		if( dir == null ) {
			return;
		}
		
		boolean exists = false;
		try {
			esu.getAllMetadata(dir);
			exists = true;
		} catch( EsuException e ) {
			if( e.getHttpCode() == 404 ) {
				// Doesn't exist
				l4j.debug( "remote object " + dir + " doesn't exist" );
			} else {
				l4j.error( "mkdirs failed for " + dir + ": " + e , e );
				return;
			}
		} catch( Exception e ) {
			l4j.error( "mkdirs failed for " + dir + ": " + e , e );
			return;
		}
		if( !exists ) {
			l4j.info( "mkdirs: " + dir );
			mkdirs( getParentDir(dir), acl );
			esu.createObjectOnPath(dir, acl, null, null, null);
		}
	}
	
	public void setAcl(Acl acl) {
		this.acl = acl;
	}

	public void setDelete(boolean delete) {
		this.delete = delete;
	}

	public void setForce(boolean force) {
		this.force = force;
	}

	private void setHosts(String[] hosts) {
		this.hosts = hosts;
	}

	private void setLocalRoot(String localroot) {
		this.localroot = new File( localroot );
	}

	public void setMeta(MetadataList meta) {
		this.meta = meta;
	}

	public void setMetadataMode(METADATA_MODE metadataMode) {
		this.metadataMode = metadataMode;
	}

	public void setMimeMap(MimetypesFileTypeMap mimeMap) {
		this.mimeMap = mimeMap;
	}

	private void setPort(int port) {
		this.port = port;
	}


	private void setProxyHost(String proxyHost) {
		this.proxyHost = proxyHost;
	}
	
	private void setProxyHttps(boolean proxyHttps) {
		this.proxyHttps = proxyHttps;
	}

	private void setProxyPassword(String proxyPassword) {
		this.proxyPassword = proxyPassword;
	}

	private void setProxyPort(int proxyPort) {
		this.proxyPort = proxyPort;
	}

	private void setProxyUsername(String proxyUser) {
		this.proxyUser = proxyUser;
	}

	private void setRemoteRoot(String remoteroot) {
		this.remoteroot = remoteroot;
	}

	private void setSecret(String secret) {
		this.secret = secret;
	}
	public void setSyncMode(MODE syncMode) {
		this.syncMode = syncMode;
	}

	private void setThreads(int threads) {
		this.threads = threads;
	}

	private void setUid(String uid) {
		this.uid = uid;
	}

	private void start() throws InterruptedException {
		this.esu = new LBEsuRestApiApache(Arrays.asList(hosts), port, uid, secret);
		
		if( proxyHost != null ) {
			if( proxyUser != null ) {
				((LBEsuRestApiApache)esu).setProxy( proxyHost, proxyPort, proxyHttps, proxyUser, proxyPassword);
			} else {
				((LBEsuRestApiApache)esu).setProxy( proxyHost, proxyPort, proxyHttps );
			}
		}
		
		mimeMap = new MimetypesFileTypeMap();
		
		// Make sure localroot exists
		if( !localroot.exists() ) {
			l4j.error( "The local root " + localroot + " does not exist!" );
			System.exit( 1 );
		}
		
		// Make sure remote path is in the correct format
		if( !remoteroot.startsWith( "/" ) ) {
			remoteroot = "/" + remoteroot;
		}
		if( !remoteroot.endsWith( "/" ) ) {
			// Must be a dir (ends with /)
			remoteroot = remoteroot + "/";
		}
		
		// Test connection to server
		try {
			String version = esu.getServiceInformation().getAtmosVersion();
			// Check server version
			if( version.startsWith("1.2") || version.startsWith("1.3") ) {
				l4j.error( "AtmosSync requires Atmos 1.4+, server is running " + version );
				System.exit( 2 );
			}
			l4j.info( "Connected to atmos " + version + " on host(s) " + Arrays.asList(hosts) );
		} catch( Exception e ) {
			l4j.error( "Error connecting to server: " + e, e );
			System.exit( 3 );
		}
		
		l4j.info( "Starting sync from " + localroot + " to " + remoteroot );
		long start = System.currentTimeMillis();
		
		queue = new LinkedBlockingQueue<Runnable>();
		pool = new ThreadPoolExecutor(threads, threads, 15, TimeUnit.SECONDS, queue);
		failedItems = Collections.synchronizedSet( new HashSet<TaskNode>() );
		
		graph = new SimpleDirectedGraph<TaskNode, DefaultEdge>(DefaultEdge.class);

		startSync();
		
		while(true) {
			synchronized (graph) {
				if( graph.vertexSet().size() == 0 ) {
					// We're done
					pool.shutdownNow();
					break;
				}
				
				// Look for available unsubmitted tasks
				BreadthFirstIterator<TaskNode, DefaultEdge> i = new BreadthFirstIterator<TaskNode, DefaultEdge>(graph);
				while( i.hasNext() ) {
					TaskNode t = i.next();
					if( graph.inDegreeOf(t) == 0 && !t.isQueued() ) {
						t.setQueued(true);
						l4j.debug( "Submitting " + t );
						pool.submit(t);
					}
				}
			}
			
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// Ignore
			}
		}
		
		long end = System.currentTimeMillis();
		long secs = ((end-start)/1000);
		if( secs == 0 ) {
			secs = 1;
		}
		
		long rate = byteCount / secs;
		System.out.println("Transferred " + byteCount + " bytes in " + secs + " seconds (" + rate + " bytes/s)" );
		System.out.println("Successful Files: " + completedCount + " Failed Files: " + failedCount );
		System.out.println("Failed Files: " + failedItems );
		
		if( failedCount > 0 ) {
			System.exit(1);
		} else {
			System.exit(0);
		}

	}

	private void startSync() {
		ObjectPath remotesync = new ObjectPath( remoteroot );
		
		if( acl != null ) {
			// Make sure root directory exists.
			mkdirs(remotesync, acl);
		}
		
		// Create the initial task
		SyncTask rootSync = new SyncTask(localroot, remotesync, this);
		rootSync.addToGraph(graph);
	}

	public synchronized void success(TaskNode task, File file, ObjectPath objectPath,
			long bytes) {
		byteCount += bytes;
		completedCount++;
		int pct = completedCount*100 / fileCount;
		l4j.info( pct + "% (" + completedCount + "/" + fileCount +") Completed: " + file );
		
	}

	public boolean isSyncingMetadata() {
		return syncingMetadata;
	}

	public void setSyncingMetadata(boolean syncingMetadata) {
		this.syncingMetadata = syncingMetadata;
	}
}
