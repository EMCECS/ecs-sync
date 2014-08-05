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
import org.springframework.beans.factory.InitializingBean;

/**
 * @author cwikj
 */
public class LoggingPlugin extends SyncPlugin implements InitializingBean {
    private static final String DEBUG_OPTION = "debug";
    private static final String DEBUG_DESC = "Enables debug messages";
    private static final String VERBOSE_OPTION = "verbose";
    private static final String VERBOSE_DESC = "Enables info messages";
    private static final String QUIET_OPTION = "quiet";
    private static final String QUIET_DESC = "Only shows warning and error messages";
    private static final String SILENT_OPTION = "silent";
    private static final String SILENT_DESC = "Does not print out any messages";

    private String level;

    @Override
    public void filter(SyncObject obj) {
        // Should never actually get called, but in case it does...
        getNext().filter(obj);
    }

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

    @Override
    public boolean parseOptions(CommandLine line) {
        if (line.hasOption(DEBUG_OPTION)) {
            level = DEBUG_OPTION;
        }
        if (line.hasOption(VERBOSE_OPTION)) {
            level = VERBOSE_OPTION;
        }
        if (line.hasOption(QUIET_OPTION)) {
            level = QUIET_OPTION;
        }
        if (line.hasOption(SILENT_OPTION)) {
            level = SILENT_OPTION;
        }

        afterPropertiesSet();

        // Always return false because we don't want this plugin to inject itself into the chain.
        return false;
    }

    @Override
    public void validateChain(SyncPlugin first) {
    }

    @Override
    public String getName() {
        return "Logging Configuration";
    }

    @Override
    public String getDocumentation() {
        return "Configures the level of output from the application. This sets the root logger level. Any other categories configured in log4j.xml, etc. are unaffected";
    }

    @Override
    public void afterPropertiesSet() {
        if (DEBUG_OPTION.equals(level)) {
            LogManager.getRootLogger().setLevel(Level.DEBUG);
        } else if (VERBOSE_OPTION.equals(level)) {
            LogManager.getRootLogger().setLevel(Level.INFO);
        } else if (QUIET_OPTION.equals(level)) {
            LogManager.getRootLogger().setLevel(Level.WARN);
        } else if (SILENT_OPTION.equals(level)) {
            LogManager.getRootLogger().setLevel(Level.FATAL);
        }
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }
}
