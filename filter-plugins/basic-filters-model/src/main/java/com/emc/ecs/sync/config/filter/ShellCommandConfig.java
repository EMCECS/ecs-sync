/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "shell-command")
@Label("Shell Command Filter")
@Documentation("Executes a shell command for each object transferred. " +
        "The command will be given one or two arguments, depending on when it is " +
        "executed: the source identifier is always the first argument. The target " +
        "identifier is the second argument only if the command is executed after " +
        "sending the object to the target (otherwise it will be null). " +
        "By default, the command will execute before sending the object to the " +
        "target storage, but that can be changed by setting the executeAfterSending option.")
public class ShellCommandConfig extends AbstractConfig {
    private String shellCommand;
    private boolean executeAfterSending;
    private boolean failOnNonZeroExit = true;
    private boolean retryOnFail;

    @Option(orderIndex = 10, required = true, valueHint = "path-to-command", description = "The shell command to execute")
    public String getShellCommand() {
        return shellCommand;
    }

    public void setShellCommand(String shellCommand) {
        this.shellCommand = shellCommand;
    }

    @Option(orderIndex = 20, description = "Specifies whether the shell command should be executed after sending the object to the target storage. By default it is executed before sending the object")
    public boolean isExecuteAfterSending() {
        return executeAfterSending;
    }

    public void setExecuteAfterSending(boolean executeAfterSending) {
        this.executeAfterSending = executeAfterSending;
    }

    @Option(orderIndex = 30, advanced = true, cliInverted = true, description = "By default, any non-zero exit status from the command will cause the object to fail the sync. Disable this option to allow a non-zero status to be marked a success. Note: if executeAfterSending is false (default) and the command returns a non-zero exit status, the object will not be sent to the target")
    public boolean isFailOnNonZeroExit() {
        return failOnNonZeroExit;
    }

    public void setFailOnNonZeroExit(boolean failOnNonZeroExit) {
        this.failOnNonZeroExit = failOnNonZeroExit;
    }

    @Option(orderIndex = 40, advanced = true, description = "By default, if failOnNonZeroExit is true, a failure of the script is flagged as NOT retryable. If you script failures to be retried, set this to true")
    public boolean isRetryOnFail() {
        return retryOnFail;
    }

    public void setRetryOnFail(boolean retryOnFail) {
        this.retryOnFail = retryOnFail;
    }
}
