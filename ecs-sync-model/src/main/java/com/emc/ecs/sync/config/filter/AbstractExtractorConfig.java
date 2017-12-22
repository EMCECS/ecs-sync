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
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Option;

public abstract class AbstractExtractorConfig extends AbstractConfig {
    private boolean fileMetadataRequired = true;

    @Option(orderIndex = 10, cliInverted = true, advanced = true, description = "by default, file metadata must be " +
            "extracted from the origin filesystem and provided in the source file list. this is the only way to get the " +
            "attributes/security info")
    public boolean isFileMetadataRequired() {
        return fileMetadataRequired;
    }

    public void setFileMetadataRequired(boolean fileMetadataRequired) {
        this.fileMetadataRequired = fileMetadataRequired;
    }
}
