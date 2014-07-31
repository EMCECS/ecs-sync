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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.Assert;

/**
 * This is an extension of the filesystem source that reads its list of 
 * files to transfer from a database SELECT query.
 * 
 * Note that this source currently only supports Spring configuration.
 *
 */
public class DbListFilesystemSource extends FilesystemSource implements InitializingBean {
	private DataSource dataSource;
	private String selectQuery;
	private List<String> metadataColumns;
	private String filenameColumn;
	
	@Override
	public void run() {
		running = true;
		initQueue();
		
		JdbcTemplate tmpl = new JdbcTemplate(dataSource);
		
		SqlRowSet rs = tmpl.queryForRowSet(selectQuery);
		
		while(rs.next()) {
			File f = new File(rs.getString(filenameColumn));
			ReadFileTask t = new ReadFileTask(f);
			
			if(metadataColumns != null) {
				Map<String, String> extraMetadata = new HashMap<String, String>();
				for(String colName : metadataColumns) {
					String value = rs.getString(colName);
					if(value == null) {
						continue;
					}
					extraMetadata.put(colName, value);
				}
				t.setExtraMetadata(extraMetadata);
			}
			
			if(f.isDirectory()) {
				submitCrawlTask(t);
			} else {
				submitTransferTask(t);
			}
		}

		runQueue();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.hasText(filenameColumn, 
				"The property 'filenameColumn' is required");
		Assert.hasText(selectQuery, "The property 'selectQuery' is required.");
		Assert.notNull(dataSource, "The property 'dataSource' is required.");
		setUseAbsolutePath(true);
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

	/**
	 * @return the metadataColumns
	 */
	public List<String> getMetadataColumns() {
		return metadataColumns;
	}

	/**
	 * @param metadataColumns the metadataColumns to set
	 */
	public void setMetadataColumns(List<String> metadataColumns) {
		this.metadataColumns = metadataColumns;
	}

	/**
	 * @return the filenameColumn
	 */
	public String getFilenameColumn() {
		return filenameColumn;
	}

	/**
	 * @param filenameColumn the filenameColumn to set
	 */
	public void setFilenameColumn(String filenameColumn) {
		this.filenameColumn = filenameColumn;
	}
}
