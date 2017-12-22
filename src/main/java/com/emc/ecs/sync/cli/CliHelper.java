/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.cli;

import com.emc.ecs.sync.config.*;
import org.apache.commons.cli.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public final class CliHelper {
    private static final CommandLineParser parser = new DefaultParser();

    public static CliConfig parseCliConfig(String[] args) throws ParseException {
        ConfigWrapper<CliConfig> wrapper = ConfigUtil.wrapperFor(CliConfig.class);
        CommandLine commandLine = parser.parse(wrapper.getOptions(), args, true);
        CliConfig cliConfig = wrapper.parse(commandLine);

        if (cliConfig.isHelp() || cliConfig.isVersion()) {
            if (cliConfig.isHelp()) System.out.print(longHelp());
            return null;
        } else {
            if (!cliConfig.isRestOnly() && cliConfig.getXmlConfig() == null
                    && (cliConfig.getSource() == null || cliConfig.getTarget() == null)) {
                throw new ParseException("Source and target options should be specified first");
            }
            return cliConfig;
        }
    }

    public static SyncConfig parseSyncConfig(CliConfig cliConfig, String[] args) throws ParseException {
        // main CLI options
        Options options = ConfigUtil.wrapperFor(CliConfig.class).getOptions();

        // sync options
        ConfigWrapper<SyncOptions> optionsWrapper = ConfigUtil.wrapperFor(SyncOptions.class);
        for (Option o : optionsWrapper.getOptions().getOptions()) {
            options.addOption(o);
        }

        // source options
        ConfigWrapper<?> sourceWrapper = ConfigUtil.storageConfigWrapperFor(cliConfig.getSource());
        if (RoleType.Target == sourceWrapper.getRole())
            throw new ParseException(sourceWrapper.getLabel() + " cannot be used as a source");
        for (Option o : sourceWrapper.getOptions("source-").getOptions()) {
            options.addOption(o);
        }

        // target options
        ConfigWrapper<?> targetWrapper = ConfigUtil.storageConfigWrapperFor(cliConfig.getTarget());
        if (RoleType.Source == targetWrapper.getRole())
            throw new ParseException(targetWrapper.getLabel() + " cannot be used as a target");
        for (Option o : targetWrapper.getOptions("target-").getOptions()) {
            options.addOption(o);
        }

        // filter options
        List<ConfigWrapper<?>> filterWrappers = new ArrayList<>();
        if (cliConfig.getFilters() != null) {
            for (String cliName : cliConfig.getFilters().split(",")) {
                ConfigWrapper<?> filterWrapper = ConfigUtil.filterConfigWrapperFor(cliName);
                filterWrappers.add(filterWrapper);
                for (Option o : filterWrapper.getOptions().getOptions()) {
                    options.addOption(o);
                }
            }
        }

        // parse the command line
        CommandLine commandLine = parser.parse(options, args);

        // map parsed command line to config objects
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setOptions(optionsWrapper.parse(commandLine));
        syncConfig.setSource(parseStorage(sourceWrapper, commandLine, "source-", cliConfig.getSource()));
        syncConfig.setTarget(parseStorage(targetWrapper, commandLine, "target-", cliConfig.getTarget()));

        List<Object> filterConfigs = new ArrayList<>();
        for (ConfigWrapper<?> filterWrapper : filterWrappers) {
            filterConfigs.add(filterWrapper.parse(commandLine));
        }
        syncConfig.setFilters(filterConfigs);

        return syncConfig;
    }

    // allow the storage plugins to parse their URIs (calls the @UriParser method)
    private static <C> C parseStorage(ConfigWrapper<C> wrapper, CommandLine commandLine, String optionPrefix, String uri) {
        C config = wrapper.parse(commandLine, optionPrefix);
        wrapper.parseUri(config, uri);
        return config;
    }

    public static String longHelp() {
        StringWriter helpWriter = new StringWriter();
        PrintWriter pw = new PrintWriter(helpWriter);
        HelpFormatter fmt = new HelpFormatter();
        fmt.setWidth(79);

        // main CLI options
        Options options = ConfigUtil.wrapperFor(CliConfig.class).getOptions();

        // sync options
        for (Option o : ConfigUtil.wrapperFor(SyncOptions.class).getOptions().getOptions()) {
            options.addOption(o);
        }

        // Make sure we do CommonOptions first
        String usage = "java -jar ecs-sync.jar -source <source-uri> [-filters <filter1>[,<filter2>,...]] -target <target-uri> [options]";
        fmt.printHelp(pw, fmt.getWidth(), usage, "Common options:", options, fmt.getLeftPadding(), fmt.getDescPadding(), null);

        pw.print("\nAvailable plugins are listed below along with any custom options they may have\n");

        // Do the rest
        for (ConfigWrapper<?> storageWrapper : ConfigUtil.allStorageConfigWrappers()) {
            pw.write('\n');
            pw.write(String.format("%s (%s)%s\n", storageWrapper.getLabel(), storageWrapper.getUriPrefix(),
                    storageWrapper.getRole() == null ? "" : " -- " + storageWrapper.getRole() + " Only"));
            fmt.printWrapped(pw, fmt.getWidth(), 4, "    " + storageWrapper.getDocumentation());
            fmt.printWrapped(pw, fmt.getWidth(), 4, "    NOTE: Storage options must be prefixed by source- or target-, depending on which role they assume");
            fmt.printOptions(pw, fmt.getWidth(), storageWrapper.getOptions(), fmt.getLeftPadding(), fmt.getDescPadding());
        }
        for (ConfigWrapper<?> filterWrapper : ConfigUtil.allFilterConfigWrappers()) {
            pw.write('\n');
            pw.write(String.format("%s (%s)\n", filterWrapper.getLabel(), filterWrapper.getCliName()));
            fmt.printWrapped(pw, fmt.getWidth(), 4, "    " + filterWrapper.getDocumentation());
            fmt.printOptions(pw, fmt.getWidth(), filterWrapper.getOptions(), fmt.getLeftPadding(), fmt.getDescPadding());
        }

        return helpWriter.toString();
    }

    private CliHelper() {
    }
}
