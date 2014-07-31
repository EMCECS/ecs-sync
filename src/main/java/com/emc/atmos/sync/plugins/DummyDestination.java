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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

/**
 * Dummy destination object that can be used to test sources or filter plugins.
 * @author cwikj
 */
public class DummyDestination extends DestinationPlugin {
	private static final Logger l4j = Logger.getLogger(DummyDestination.class);
	
	public static final String SINK_DATA_OPTION = "sink-data";
	public static final String SINK_DATA_DESC = "Read all data from the input stream";
	private boolean sinkData;

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
	 */
	@Override
	public void filter(SyncObject obj) {
		try {
			obj.setDestURI(new URI("file:///dev/null"));
		} catch (URISyntaxException e) {
			throw new RuntimeException(
					"Failed to build dest URI: " + e.getMessage(), e);
		}
		if(sinkData) {
			LogMF.debug(l4j, "Sinking source object {0}", obj.getSourceURI());
			byte[] buffer = new byte[4096];
			InputStream in = null;
			try {
				in = obj.getInputStream();
				while(in != null && in.read(buffer) != -1) {
					// Do nothing!
				}
			} catch (IOException e) {
				throw new RuntimeException(
						"Failed to read input stream: " + e.getMessage(), e);
			} finally {
				if(in != null) {
					try {
						in.close();
					} catch (IOException e) {
						//Ignore
					}
				}
			}
		}
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		opts.addOption(OptionBuilder.withDescription(SINK_DATA_DESC)
				.withLongOpt(SINK_DATA_OPTION).create());
		
		return opts;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		if("dummy".equals(line.getOptionValue(CommonOptions.DESTINATION_OPTION))) {
			if(line.hasOption(SINK_DATA_OPTION)) {
				sinkData = true;
			}
			return true;
		}
		
		return false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
		// No known plugins this is incompatible with.
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "Dummy Destination";
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "The dummy destination simply discards any data received.  With" +
				" the --sink-data option it will also read all data from any " +
				"input streams and discard that too.  This plugin is mainly " +
				"used for testing sources and filters.  It is activated by " +
				"using the destination 'dummy'";
	}

	public boolean isSinkData() {
		return sinkData;
	}

	public void setSinkData(boolean sinkData) {
		this.sinkData = sinkData;
	}

}
