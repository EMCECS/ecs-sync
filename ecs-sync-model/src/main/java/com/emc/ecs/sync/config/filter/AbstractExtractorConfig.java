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
