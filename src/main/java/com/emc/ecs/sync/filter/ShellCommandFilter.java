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

import com.emc.ecs.sync.config.filter.ShellCommandConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Implements a plugin that executes a shell command after an object is
 * transferred
 */
public class ShellCommandFilter extends AbstractFilter<ShellCommandConfig> {
    private static final Logger log = LoggerFactory.getLogger(ShellCommandFilter.class);

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (config.getShellCommand() == null)
            throw new ConfigurationException("you must specify a shell command to run");
        File shellFile = new File(config.getShellCommand());
        if (!shellFile.exists())
            throw new ConfigurationException(config.getShellCommand() + " does not exist");
        if (!shellFile.canExecute())
            throw new ConfigurationException(config.getShellCommand() + " is not executable");
    }

    @Override
    public void filter(ObjectContext objectContext) {
        getNext().filter(objectContext);

        String[] cmdLine = new String[]{config.getShellCommand(),
                objectContext.getSourceSummary().getIdentifier(), objectContext.getTargetId()};
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
    public SyncObject reverseFilter(ObjectContext objectContext) {
        log.warn("This filter is not aware of modifications performed by the shell command, verification may not be accurate");
        return getNext().reverseFilter(objectContext);
    }

    private void drain(InputStream in, PrintStream out) throws IOException {
        while (in.available() > 0) {
            out.print((char) in.read());
        }
    }
}
