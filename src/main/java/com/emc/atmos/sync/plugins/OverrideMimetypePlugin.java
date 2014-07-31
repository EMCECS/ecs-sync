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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * @author cwikj
 *
 */
public class OverrideMimetypePlugin extends SyncPlugin {
	private static final String OVERRIDE_MIMETYPE_OPTION = "override-mimetype";
	private static final String OVERRIDE_MIMETYPE_DESC = "enables the " +
			"override mimetype plugin that will override the mimetype of " +
			"an object.";
	private static final String OVERRIDE_MIMETYPE_ARG_NAME = "mimetype";
	private static final String FORCE_MIMETYPE_OPTION = "force-mimetype";
	private static final String FORCE_MIMETYPE_DESC = "If specified, the " +
			"mimetype will be overwritten regardless of its prior value.";
	
	private String mimeType;
	private boolean force;

	@Override
	public void filter(SyncObject obj) {
		if(force) {
			obj.getMetadata().setContentType(mimeType);
		} else {
			if(obj.getMetadata().getContentType() == null || 
					obj.getMetadata().getContentType().equals(
							"application/octet-stream")) {
				obj.getMetadata().setContentType(mimeType);
			}
		}
		
		getNext().filter(obj);
	}

	/* (non-Javadoc)
	 * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
	 */
	@SuppressWarnings("static-access")
	@Override
	public Options getOptions() {
		Options opts = new Options();
		opts.addOption(OptionBuilder
				.withLongOpt(OVERRIDE_MIMETYPE_OPTION)
				.withDescription(OVERRIDE_MIMETYPE_DESC)
				.hasArg().withArgName(OVERRIDE_MIMETYPE_ARG_NAME).create());
		
		opts.addOption(OptionBuilder
				.withLongOpt(FORCE_MIMETYPE_OPTION)
				.withDescription(FORCE_MIMETYPE_DESC).create());
		
		return opts;
	}

	@Override
	public boolean parseOptions(CommandLine line) {
		if(!line.hasOption(OVERRIDE_MIMETYPE_OPTION)) {
			return false;
		}
		mimeType = line.getOptionValue(OVERRIDE_MIMETYPE_OPTION);
		
		force = line.hasOption(FORCE_MIMETYPE_OPTION);
		
		return true;
	}

	@Override
	public void validateChain(SyncPlugin first) {
	}

	@Override
	public String getName() {
		return "Override Mimetype";
	}

	@Override
	public String getDocumentation() {
		return "This plugin allows you to override the default mimetype " +
				"of objects getting transferred.  It is useful for instances " +
				"where the mimetype of an object cannot be inferred from " +
				"its extension or is nonstandard (not in Java's " +
				"mime.types file).  You can also use the force option to " +
				"override the mimetype of all objects.";
	}

}
