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
@FilterConfig(cliName = "id-logging")
@Label("ID Logging Filter")
@Documentation("Logs the input and output Object IDs to a file. These IDs " +
        "are specific to the source and target plugins")
public class IdLoggingConfig extends AbstractConfig {
    private String idLogFile;

    @Option(orderIndex = 10, required = true, valueHint = "path-to-file", description = "The path to the file to log IDs to")
    public String getIdLogFile() {
        return idLogFile;
    }

    public void setIdLogFile(String idLogFile) {
        this.idLogFile = idLogFile;
    }
}
