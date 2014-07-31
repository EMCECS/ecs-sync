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
import java.io.PrintStream;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * Implements a plugin that executes a shell command after an object is 
 * transferred
 */
public class ShellCommandPlugin extends SyncPlugin {
	private String command;
	
	private static final String SHELL_COMMAND_OPT = "shell-command";
	private static final String SHELL_COMMAND_DESC = "Activates the shell " +
			"command plugin.  The argument should be the path to the " +
			"command to execute.";
	private static final String SHELL_COMMAND_ARG = "path-to-command";
	

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
	 */
	@Override
	public void filter(SyncObject obj) {
		getNext().filter(obj);
		String[] cmdLine = new String[] { command, 
				obj.getSourceURI().toString(), obj.getDestURI().toString() };
		try {
			Process p = Runtime.getRuntime().exec(cmdLine);
			
			InputStream stdout = p.getInputStream();
			InputStream stderr = p.getErrorStream();
			while(true) {
				try {
					int exitCode = p.exitValue();
					if(exitCode != 0) {
						throw new RuntimeException("Command: " +
								Arrays.asList(cmdLine) + 
								"exited with code " + exitCode);				
					} else {
						return;
					}
				} catch(IllegalThreadStateException e){
					// ignore; process running
				}
				
				// Drain stdout and stderr.  Many processes will hang if you
				// dont do this.
				drain(stdout, System.out);
				drain(stderr, System.err);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error executing command: " +
					Arrays.asList(cmdLine) + ": " + e.getMessage(), e);
		}
	}

	private void drain(InputStream in, PrintStream out) throws IOException {
		while(in.available()>0) {
			out.print((char)in.read());
		}
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options o = new Options();
		
		o.addOption(OptionBuilder.withLongOpt(SHELL_COMMAND_OPT)
				.withDescription(SHELL_COMMAND_DESC)
				.hasArg().withArgName(SHELL_COMMAND_ARG).create());
		
		return o;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
	 */
	@Override
	public boolean parseOptions(CommandLine line) {
		if(!line.hasOption(SHELL_COMMAND_OPT)) {
			return false;
		}
		command = line.getOptionValue(SHELL_COMMAND_OPT);
		
		return true;
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#validateChain(com.emc.atmos.sync.plugins.SyncPlugin)
	 */
	@Override
	public void validateChain(SyncPlugin first) {
	}

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
	 */
	@Override
	public String getName() {
		return "Shell Command Plugin";
	}

	/**
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
	 */
	@Override
	public String getDocumentation() {
		return "Executes a shell command after each successful transfer.  " +
				"The command will be given two arguments: the source URI " +
				"and the destination URI.";
	}

}
