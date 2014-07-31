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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import com.emc.esu.api.Identifier;
import com.emc.esu.api.ObjectMetadata;
import com.google.gson.Gson;

/**
 * Downloads metadata from Atmos to a local file.
 */
public class AtmosDownloadMetaTask extends TaskNode {

	private Identifier id;
	private File file;
	private AtmosSync sync;

	public AtmosDownloadMetaTask(Identifier id, File file, AtmosSync sync) {
		this.id = id;
		this.file = file;
		this.sync = sync;
	}

	/**
	 * @see com.emc.atmos.sync.TaskNode#execute()
	 */
	@Override
	protected TaskResult execute() throws Exception {
		try {
			ObjectMetadata om = sync.getEsu().getAllMetadata(id);
			
			if(!file.getParentFile().exists()) {
				file.getParentFile().mkdirs();
			}
			
			Gson gson = new Gson();
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			try {
				gson.toJson(om, writer);
			} finally {
				writer.close();
			}
		} catch(Exception e) {
			sync.failure(this, file, id, e);
			return new TaskResult(false);
		}
		
		return new TaskResult(true);
	}

}
