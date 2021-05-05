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
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@FilterConfig(cliName = "cas-extractor")
@Label("CAS Single Blob Extractor")
@Documentation("Extracts a single blob from each CAS clip (must use CAS source). If more than one blob is found in a clip, " +
        "an error is thrown and the clip will not be migrated. Please specify what should be used as the relative path/name of " +
        "each object: the clip ID (CA of the clip), the value of a tag attribute, or provided in the source list file " +
        "(in CSV format: {clip-id},{relative-path}). Tag attributes will be migrated as user metadata, but names are " +
        "limited to the US-ASCII charset - choose an appropriate behavior for migrating invalid attribute names. " +
        "NOTE: When changing protocols, applications must be updated to integrate with the new protocol and database " +
        "references may need updating to use the new object identifiers.")
public class CasSingleBlobExtractorConfig extends AbstractConfig {
    private PathSource pathSource = PathSource.ClipId;
    private String pathAttribute;
    private AttributeNameBehavior attributeNameBehavior = AttributeNameBehavior.FailTheClip;
    private boolean missingBlobsAreEmptyFiles = true;

    @Option(orderIndex = 10, description = "Identifies where the path information for the object is stored. Can be pulled from the source list file as the 2nd CSV column, or from an attribute value somewhere in the clip, or it can just be the clip ID (CA). Default is the clip ID")
    public PathSource getPathSource() {
        return pathSource;
    }

    public void setPathSource(PathSource pathSource) {
        this.pathSource = pathSource;
    }

    @Option(orderIndex = 20, valueHint = "path-attribute-name", description = "The name of the tag attribute that holds the path. Use with pathSource: Attribute")
    public String getPathAttribute() {
        return pathAttribute;
    }

    public void setPathAttribute(String pathAttribute) {
        this.pathAttribute = pathAttribute;
    }

    @Option(orderIndex = 30, cliInverted = true, advanced = true, description = "By default, if a clip does not have a blob and meets all other criteria, it will be treated as an empty file. Disable this to fail the clip in that case")
    public boolean isMissingBlobsAreEmptyFiles() {
        return missingBlobsAreEmptyFiles;
    }

    public void setMissingBlobsAreEmptyFiles(boolean missingBlobsAreEmptyFiles) {
        this.missingBlobsAreEmptyFiles = missingBlobsAreEmptyFiles;
    }

    @Option(orderIndex = 40, description = "Indicate how to handle attribute name characters outside US-ASCII charset. When bad characters are encountered, you can fail the clip (don't migrate clip), skip moving the bad attribute name as user metadata (still migrating clip) or replace bad attribute name characters with '-' (still migrating clip). If character replacement is necessary, original attribute names will be saved in the \"x-emc-invalid-meta-names\" field as a comma-delimited list")
    public AttributeNameBehavior getAttributeNameBehavior() {
        return attributeNameBehavior;
    }

    public void setAttributeNameBehavior(AttributeNameBehavior attributeNameBehavior) {
        this.attributeNameBehavior = attributeNameBehavior;
    }

    @XmlEnum
    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    public enum PathSource {
        ClipId, Attribute, CSV
    }

    @XmlEnum
    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    public enum AttributeNameBehavior {
        FailTheClip, SkipBadName, ReplaceBadCharacters
    }
}