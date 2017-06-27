package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "dx-extractor")
@Label("DX Extraction Filter")
@Documentation("Extracts DX file data directly from the backing storage system. NOTE: this filter requires a " +
        "specifically formatted CSV file as the source list file. " +
        "The format is: [source-id],[relative-path-name],[cifs-ecs-encoding],[original-name],[file-attributes],[security-descriptor]")
public class DxExtractorConfig extends AbstractExtractorConfig {
}
