/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.ConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Implements a plugin that executes a shell command after an object is
 * transferred
 */
public class ShellCommandFilter extends SyncFilter {
    private static final Logger log = LoggerFactory.getLogger(ShellCommandFilter.class);

    public static final String ACTIVATION_NAME = "shell-command";

    private static final String SHELL_COMMAND_OPT = "shell-command";
    private static final String SHELL_COMMAND_DESC = "The shell command to execute.";
    private static final String SHELL_COMMAND_ARG = "path-to-command";

    private String command;

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options o = new Options();
        o.addOption(Option.builder().longOpt(SHELL_COMMAND_OPT).desc(SHELL_COMMAND_DESC)
                .hasArg().argName(SHELL_COMMAND_ARG).build());
        return o;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        if (line.hasOption(SHELL_COMMAND_OPT))
            command = line.getOptionValue(SHELL_COMMAND_OPT);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (command == null)
            throw new ConfigurationException("you must specify a shell command to run");
    }

    @Override
    public void filter(SyncObject obj) {
        getNext().filter(obj);

        String[] cmdLine = new String[]{command, obj.getSourceIdentifier(), obj.getTargetIdentifier()};
        try {
            Process p = Runtime.getRuntime().exec(cmdLine);

            InputStream stdout = p.getInputStream();
            InputStream stderr = p.getErrorStream();
            while (true) {
                try {
                    int exitCode = p.exitValue();
                    if (exitCode != 0) {
                        throw new RuntimeException("Command: " + Arrays.asList(cmdLine) + "exited with code " + exitCode);
                    } else {
                        return;
                    }
                } catch (IllegalThreadStateException e) {
                    // ignore; process running
                }

                // Drain stdout and stderr.  Many processes will hang if you
                // dont do this.
                drain(stdout, System.out);
                drain(stderr, System.err);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error executing command: " + Arrays.asList(cmdLine) + ": " + e.getMessage(), e);
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        log.warn("This filter is not aware of modifications performed by the shell command, verification may not be accurate");
        return getNext().reverseFilter(obj);
    }

    private void drain(InputStream in, PrintStream out) throws IOException {
        while (in.available() > 0) {
            out.print((char) in.read());
        }
    }

    @Override
    public String getName() {
        return "Shell Command Filter";
    }

    @Override
    public String getDocumentation() {
        return "Executes a shell command after each successful transfer.  " +
                "The command will be given two arguments: the source identifier " +
                "and the target identifier.";
    }
}
