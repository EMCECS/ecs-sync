package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "cifs-ecs-ingester")
@Label("CIFS-ECS Ingest Filter")
@Documentation("Ingests CIFS attribute and security descriptor metadata so it is compatible with CIFS-ECS. NOTE: this filter requires a " +
        "specifically formatted CSV file as the source list file. " +
        "The format is: [source-id],[relative-path-name],[cifs-ecs-encoding],[original-name],[file-attributes],[security-descriptor]")
public class CifsEcsConfig extends AbstractConfig {
    private boolean fileMetadataRequired = true;

    @Option(orderIndex = 10, cliInverted = true, advanced = true, description = "by default, file metadata must be " +
            "extracted from the source CIFS share and provided in the source file list. this is the only way to get the " +
            "CIFS security descriptor and extended attributes")
    public boolean isFileMetadataRequired() {
        return fileMetadataRequired;
    }

    public void setFileMetadataRequired(boolean fileMetadataRequired) {
        this.fileMetadataRequired = fileMetadataRequired;
    }
}
