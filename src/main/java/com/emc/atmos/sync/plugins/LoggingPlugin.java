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
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author cwikj
 *
 */
public class LoggingPlugin extends SyncPlugin {
    private static final Logger l4j = Logger.getLogger(LoggingPlugin.class);
    
    private static final String DEBUG_OPTION = "debug";
    private static final String DEBUG_DESC = "Enables debug messages";
    private static final String VERBOSE_OPTION = "verbose";
    private static final String VERBOSE_DESC = "Enables info messages";
    private static final String QUIET_OPTION = "quiet";
    private static final String QUIET_DESC = "Only shows warning and error messages";
    private static final String SILENT_OPTION = "silent";
    private static final String SILENT_DESC = "Does not print out any messages";

    /* (non-Javadoc)
     * @see com.emc.atmos.sync.plugins.SyncPlugin#filter(com.emc.atmos.sync.plugins.SyncObject)
     */
    @Override
    public void filter(SyncObject obj) {
        // Should never actually get called, but in case it does...
        getNext().filter(obj);

    }

    /* (non-Javadoc)
     * @see com.emc.atmos.sync.plugins.SyncPlugin#getOptions()
     */
    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        Options opts = new Options();
        
        OptionGroup debugOpts = new OptionGroup();
        debugOpts.addOption(OptionBuilder.withLongOpt(DEBUG_OPTION)
                .withDescription(DEBUG_DESC).create());
        debugOpts.addOption(OptionBuilder.withLongOpt(VERBOSE_OPTION)
                .withDescription(VERBOSE_DESC).create());
        debugOpts.addOption(OptionBuilder.withLongOpt(SILENT_OPTION)
                .withDescription(SILENT_DESC).create());
        debugOpts.addOption(OptionBuilder.withLongOpt(QUIET_OPTION)
                .withDescription(QUIET_DESC).create());
        opts.addOptionGroup(debugOpts);
        
        return opts;
    }

    /* (non-Javadoc)
     * @see com.emc.atmos.sync.plugins.SyncPlugin#parseOptions(org.apache.commons.cli.CommandLine)
     */
    @Override
    public boolean parseOptions(CommandLine line) {
        if(line.hasOption(DEBUG_OPTION)) {
            LogManager.getRootLogger().setLevel(Level.DEBUG);
        }
        if(line.hasOption(VERBOSE_OPTION)) {
            LogManager.getRootLogger().setLevel(Level.INFO);
        }
        if(line.hasOption(QUIET_OPTION)) {
            LogManager.getRootLogger().setLevel(Level.WARN);
        }
        if(line.hasOption(SILENT_OPTION)) {
            LogManager.getRootLogger().setLevel(Level.FATAL);
        }
        
        // Always return false because we don't want this plugin to inject itself into the chain.
        return false;
    }

    /* (non-Javadoc)
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
        return "Logging Configuration";
    }

    /* (non-Javadoc)
     * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
     */
    @Override
    public String getDocumentation() {
        return "Configures the level of output from the application. This sets the root logger level. Any other categories configured in log4j.xml, etc. are unaffected";
    }

}
