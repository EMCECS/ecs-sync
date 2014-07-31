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

import com.emc.atmos.api.ObjectId;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.sync.TaskNode;
import com.emc.atmos.sync.TaskResult;
import com.emc.atmos.sync.util.CountingInputStream;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
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
public class SqlBlobSource extends MultithreadedGraphSource {
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
    private int bufferSize = CommonOptions.DEFAULT_BUFFER_SIZE;

	private Connection con = null;
	private PreparedStatement ps = null;
	private ResultSet rs = null;

	/**
	 * @see com.emc.atmos.sync.plugins.SourcePlugin#run()
	 */
	@Override
	public void run() {
		running = true;
		initQueue();

		try {
			// We execute the query here for two reasons
			// 1- easier to report errors here than from inside a thread
			// 2- we need to keep the resources open until all rows are
			// processed. The SelectTask will end before all the objects it
			// creates have been processed.
			con = dataSource.getConnection();
			ps = con.prepareStatement(selectSql);
			rs = ps.executeQuery();

			SelectTask st = new SelectTask();
			st.addToGraph(graph);

			runQueue();
		} catch (SQLException e) {
			throw new RuntimeException("Failed to execute query: "
					+ e.getMessage(), e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// ignore
				}
			}
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

	/**
	 * @see com.emc.atmos.sync.plugins.SourcePlugin#terminate()
	 */
	@Override
	public void terminate() {
		running = false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@Override
	public Options getOptions() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
		// Make sure the destination is Atmos
		SyncPlugin last = first;
		while (last.getNext() != null) {
			last = last.getNext();
		}

		if (!(last instanceof AtmosDestination)) {
			throw new IllegalArgumentException(
					"Destination plugin must be AtmosDestination");
		}

		if (((AtmosDestination) last).getDestNamespace() != null) {
			throw new IllegalArgumentException(
					"Destination plugin must be in Object mode not Namespace");
		}
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * The SelectTask reads rows from the ResultSet, creates SyncObjects and
	 * starts SqlSyncTasks. Doing this inside a TaskNode ensures that runQueue
	 * does not exit until this process is complete.
	 */
	class SelectTask extends TaskNode {

		@Override
		protected TaskResult execute() throws Exception {
			while (rs.next()) {
				// Make sure the graph doesn't get too big.
				while (queue.size() > threadCount * 10) {
					Thread.sleep(500);
				}

				try {
					SqlSyncObject sso = new SqlSyncObject(
							rs.getObject(sourceIdColumn),
							rs.getBlob(sourceBlobColumn));
					if (sourceAtmosIdColumn != null) {
						String soid = rs.getString(sourceAtmosIdColumn);
						if(soid != null && soid.trim().length()>0) {
							ObjectId oid = new ObjectId(
									rs.getString(sourceAtmosIdColumn));
							DestinationAtmosId dai = new DestinationAtmosId(oid);
							sso.addAnnotation(dai);
						}
					}
	
					// Metadata
					if (metadataMapping != null) {
						Map<String, Metadata> metaMap = sso.getMetadata().getMetadata();
						for (String dbName : metadataMapping.keySet()) {
							String atmosName = metadataMapping.get(dbName);
							String value = rs.getString(dbName);
							if (value == null) {
								continue;
							}
							if (metadataTrim) {
								value = value.trim();
							}

                            metaMap.put(atmosName, new Metadata(atmosName, value, false));
						}
					}
	
					// Start the task.
					SqlSyncTask sst = new SqlSyncTask(sso);
					sst.addToGraph(graph);
				} catch(Exception e) {
					l4j.error("Error starting sync object: " + e.getMessage(), e);
				}
			}
			return TaskResult.SUCCESS;
		}

	}

	/**
	 * SyncObject subclass for handling SQL blobs.
	 */
	class SqlSyncObject extends SyncObject {
		CountingInputStream cis;
		private Object sqlId;
		private Blob blob;

		public SqlSyncObject(Object sqlId, Blob blob) throws URISyntaxException, SQLException {
			this.sqlId = sqlId;
			this.blob = blob;
			setSize(blob.length());

			// Required for logging purposes.
			setSourceURI(new URI("sql://" + sqlId.toString().trim()));
		}

		@Override
		public InputStream getInputStream() {
			if (cis == null) {
				try {
					cis = new CountingInputStream(new BufferedInputStream(blob.getBinaryStream(), bufferSize));
				} catch (SQLException e) {
					throw new RuntimeException("Failed to get Blob stream: "
							+ e.getMessage(), e);
				}
			}
			return cis;
		}

		@Override
		public String getRelativePath() {
			return null;
		}

		@Override
		public long getBytesRead() {
			if (cis != null) {
				return cis.getBytesRead();
			} else {
				return 0;
			}
		}

		/**
		 * @return the sqlId
		 */
		public Object getSqlId() {
			return sqlId;
		}

	}

	class SqlSyncTask extends TaskNode {

		private SqlSyncObject sso;

		public SqlSyncTask(SqlSyncObject sso) {
			this.sso = sso;
		}

		@Override
		protected TaskResult execute() throws Exception {
			// Send down to the destination
			
			try {
				getNext().filter(sso);
			} catch(Throwable t) {
				failed(sso, t);
			}
			
			if(updateSql != null) {
				ObjectId id = ((DestinationAtmosId)sso.getAnnotation(DestinationAtmosId.class)).getId();
				// Run the update SQL
				Connection con = null;
				PreparedStatement ps = null;
				
				try {
					
					con = dataSource.getConnection();
					ps = con.prepareStatement(updateSql);
					ps.setObject(updateIdColumn, sso.getSqlId());
					ps.setString(updateAtmosIdColumn, id.toString());
					
					ps.executeUpdate();
				} catch(Exception e) {
					l4j.error("Failed to execute SQL update after sending to Atmos", e);
					l4j.warn("Atmos Object ID: " + id);
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
			
			complete(sso);
				
			return TaskResult.SUCCESS;
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

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
