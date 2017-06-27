package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "cua-extractor")
@Label("CUA Extraction Filter")
@Documentation("Extracts CUA files directly from CAS clips (must use CAS source). NOTE: this filter requires a " +
        "specifically formatted CSV file as the source list file. " +
        "For NFS, the format is: [source-id],[relative-path-name],NFS,[uid],[gid],[mode],[mtime],[ctime],[atime],[symlink-target]" +
        "For CIFS, the format is: [source-id],[relative-path-name],[cifs-ecs-encoding],[original-name],[file-attributes],[security-descriptor]")
public class CuaExtractorConfig extends AbstractExtractorConfig {
}
