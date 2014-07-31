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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * Logs the Input IDs to Output IDs
 * @author cwikj
 */
public class IdLoggerPlugin extends SyncPlugin {
	public static final String IDLOG_OPTION = "id-log-file";
	public static final String IDLOG_DESC = "The path to the file to log IDs to";
	public static final String IDLOG_ARG_NAME = "filename";
	
	private String filename;
	private PrintWriter out;

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
	 */
	@Override
	public synchronized void filter(SyncObject obj) {
		try {
			if(out == null) {
				out = new PrintWriter(new BufferedWriter(new FileWriter(new File(filename))));
			}
		} catch(IOException e) {
			throw new RuntimeException("Error writing to ID log file: " + e.getMessage(), e);
		}

		try {
			getNext().filter(obj);
			out.println(obj.getSourceURI().toASCIIString() + ", " + obj.getDestURI().toASCIIString() );
		} catch(RuntimeException e) {
			// Log the error
			out.println(obj.getSourceURI().toASCIIString() + ", FAILED: " + e.getMessage() );
			throw e;
		}
		
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		
		opts.addOption(OptionBuilder.withDescription(IDLOG_DESC)
				.withLongOpt(IDLOG_OPTION).hasArg()
				.withArgName(IDLOG_ARG_NAME).create());
		
		return opts;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		if(line.hasOption(IDLOG_OPTION)) {
			setFilename(line.getOptionValue(IDLOG_OPTION));
			return true;
		}
		return false;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
	}
	
	@Override
	public void cleanup() {
		if(out != null) {
			out.close();
			out = null;
		}
		
		super.cleanup();
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "ID Logger";
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "Logs the input and output Object IDs to a file.  These IDs " +
				"are fully-qualified URIs and are specific to the source " +
				"and destination plugins.";
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

}
