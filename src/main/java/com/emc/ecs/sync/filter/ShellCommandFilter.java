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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.NonRetriableException;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.ShellCommandConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implements a plugin that executes a shell command for each object
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
        if (config.isExecuteAfterSending()) {
            // send the object to target first, then execute command
            getNext().filter(objectContext);
        }

        // construct command line
        List<String> cmdLine = new ArrayList<String>();
        cmdLine.add(config.getShellCommand());
        cmdLine.add(objectContext.getSourceSummary().getIdentifier());
        // we will only have the target ID if we execute *after* sending
        if (config.isExecuteAfterSending()) cmdLine.add(objectContext.getTargetId());
        String cmdLineStr = cmdLine.toString();

        try {
            // execute command
            log.info("executing shell command: {}", cmdLineStr);
            Process p = Runtime.getRuntime().exec(cmdLine.toArray(new String[0]));

            try (InputStream stdout = p.getInputStream();
                 InputStream stderr = p.getErrorStream()) {

                // wait for command to complete
                int exitCode = p.waitFor();

                // Drain stdout and stderr.  Many processes will hang if you dont do this.
                String stdoutStr = drainToString(stdout);
                String stderrStr = drainToString(stderr);
                log.info("STDOUT: {}", stdoutStr);
                log.info("STDERR: {}", stderrStr);

                // handle non-zero exit status
                if (exitCode != 0) {
                    String message = String.format("Command %s exited with code %d and stderr: %s",
                            cmdLineStr, exitCode, stderrStr);
                    if (config.isFailOnNonZeroExit()) {
                        // fail the object by throwing an exception
                        if (config.isRetryOnFail()) {
                            throw new RuntimeException(message);
                        } else {
                            // script failures should not be retried
                            throw new NonRetriableException(message);
                        }
                    } else {
                        // otherwise, log a warning
                        log.warn("Command {} exited with code {} and stderr: {}", cmdLineStr, exitCode, stderrStr);
                    }
                }

            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted while executing command: " + cmdLineStr, e);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error executing command: " + cmdLineStr, e);
        }

        if (!config.isExecuteAfterSending()) {
            // send object to target only after successful command execution
            getNext().filter(objectContext);
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        log.warn("This filter is not aware of modifications performed by the shell command; verification may not be accurate");
        return getNext().reverseFilter(objectContext);
    }

    private String drainToString(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int len; (len = in.read(buffer)) != -1; ) {
            baos.write(buffer, 0, len);
        }
        return baos.toString(StandardCharsets.UTF_8.name());
    }
}
