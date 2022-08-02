/*
 * Copyright (c) 2021-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "path-mapping")
@Label("Path Mapping Filter")
@Documentation("Maps object paths between source and target. The mapping can be specified by " +
        "a CSV source list file, or by a user metadata value stored on the source object, or by providing a " +
        "regular-expression-based replacement. Note that the mapping for each object will output " +
        "the new *relative path* of the object in the target. This is the path relative to the configured target storage " +
        "location. For example, suppose you are using filesystem plugins where the configured source location is " +
        "\"/mnt/nfs1\" and the configured target location is \"/mnt/nfs2\". If you want to map the \"/mnt/nfs1/foo\" " +
        "file in the source to \"/mnt/nfs2/bar\" in the target, you would have an entry in the source list file (in CSV " +
        "format) like so: \"/mnt/nfs1/foo\",\"bar\" (be sure to quote each value in case they contain commas!). " +
        "This will change the relative path of the object from \"foo\" to " +
        "\"bar\", which will get written under the target storage as \"/mnt/nfs2/bar\". If you were using metadata to " +
        "map the objects, then the metadataName you specify should contain the target relative path as its value " +
        "(just \"bar\" in the example above). If you are using regular expressions, note that the pattern is applied to the " +
        "relative path, not the source identifier. So in the example above, your pattern could be \"foo\" and " +
        "the replacement could be \"bar\", which would replace any occurrence of \"foo\" with \"bar\" in the " +
        "relative path of each object, but the pattern will not apply to the full source location (it will not see " +
        "\"/mnt/nfs1/\"). The important thing to remember is that the mapping applies to the relative path, not the " +
        "full identifier. PLEASE NOTE: when mapping identifiers, it will not be possible to verify " +
        "object names between source and target because they will change. Be sure that your application and data " +
        "consumers are aware that this mapping has occurred.")
public class PathMappingConfig {
    public static final String META_PREVIOUS_NAME = "x-emc-previous-name";

    private MapSource mapSource = MapSource.CSV;
    private String metadataName;
    private String regExPattern;
    private String regExReplacementString;
    private boolean storePreviousPathAsMetadata = true;

    @Option(orderIndex = 10, required = true, description = "Identifies where the mapping information for each object is stored. Can be pulled from the source list file as the 2nd CSV column, or from a user metadata value stored with each object, or it can be a regular expression replacement")
    public MapSource getMapSource() {
        return mapSource;
    }

    public void setMapSource(MapSource mapSource) {
        this.mapSource = mapSource;
    }

    @Option(orderIndex = 20, description = "The name of the metadata on each object that holds its target relative path. Use with mapSource: Metadata")
    public String getMetadataName() {
        return metadataName;
    }

    public void setMetadataName(String metadataName) {
        this.metadataName = metadataName;
    }

    @Option(orderIndex = 30, description = "The regular expression pattern to use against the relative path. Use with mapSource: RegEx and in combination with regExReplacementString. This should follow the standard Java regex format (https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html). Be sure to test the pattern and the replacement thoroughly before using them in a real migration")
    public String getRegExPattern() {
        return regExPattern;
    }

    public void setRegExPattern(String regExPattern) {
        this.regExPattern = regExPattern;
    }

    @Option(orderIndex = 40, description = "The regex replacement string to use that will generate the target relative path based on the search pattern in regExPattern. Use with mapSource: RegEx. This should follow the standard Java regex replacement conventions (https://docs.oracle.com/javase/8/docs/api/java/util/regex/Matcher.html#appendReplacement-java.lang.StringBuffer-java.lang.String-). Be sure to test the pattern and the replacement thoroughly before using them in a real migration")
    public String getRegExReplacementString() {
        return regExReplacementString;
    }

    public void setRegExReplacementString(String regExReplacementString) {
        this.regExReplacementString = regExReplacementString;
    }

    @Option(orderIndex = 50, advanced = true, cliInverted = true, description = "Enable this option to store the original pathname as metadata on the target object under the key \"" + META_PREVIOUS_NAME + "\"")
    public boolean isStorePreviousPathAsMetadata() {
        return storePreviousPathAsMetadata;
    }

    public void setStorePreviousPathAsMetadata(boolean storePreviousPathAsMetadata) {
        this.storePreviousPathAsMetadata = storePreviousPathAsMetadata;
    }

    public enum MapSource {
        CSV, Metadata, RegEx
    }
}
