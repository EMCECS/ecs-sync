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
@FilterConfig(cliName = "metadata")
@Label("Metadata Filter")
@Documentation("Allows adding regular and listable (Atmos only) metadata to each object.")
public class MetadataConfig extends AbstractConfig {
    private String[] addMetadata;
    private String[] addListableMetadata;
    private String[] removeMetadata;
    private boolean removeAllUserMetadata;
    private String[] changeMetadataKeys;

    @Option(orderIndex = 10, valueHint = "name=value", description = "Adds regular metadata to every object. You can specify multiple name/value pairs by repeating the CLI option or using multiple lines in the UI form.")
    public String[] getAddMetadata() {
        return addMetadata;
    }

    public void setAddMetadata(String[] addMetadata) {
        this.addMetadata = addMetadata;
    }

    @Option(orderIndex = 20, advanced = true, valueHint = "name=value", description = "Adds listable metadata to every object. You can specify multiple name/value pairs by repeating the CLI option or using multiple lines in the UI form.")
    public String[] getAddListableMetadata() {
        return addListableMetadata;
    }

    public void setAddListableMetadata(String[] addListableMetadata) {
        this.addListableMetadata = addListableMetadata;
    }

    @Option(orderIndex = 30, advanced = true, valueHint = "name,name,...", description = "Removes metadata from every object. You can specify multiple names by repeating the CLI option or using multiple lines in the UI form.")
    public String[] getRemoveMetadata() {
        return removeMetadata;
    }

    public void setRemoveMetadata(String[] removeMetadata) {
        this.removeMetadata = removeMetadata;
    }

    @Option(orderIndex = 40, advanced = true, description = "Removes *all* user metadata from every object.")
    public boolean isRemoveAllUserMetadata() {
        return removeAllUserMetadata;
    }

    public void setRemoveAllUserMetadata(boolean removeAllUserMetadata) {
        this.removeAllUserMetadata = removeAllUserMetadata;
    }

    @Option(orderIndex = 50, advanced = true, valueHint = "oldName=newName", description = "Changes metadata keys on every object. You can specify multiple old/new key names by repeating the CLI option or using multiple lines in the UI form.")
    public String[] getChangeMetadataKeys() {
        return changeMetadataKeys;
    }

    public void setChangeMetadataKeys(String[] changeMetadataKeys) {
        this.changeMetadataKeys = changeMetadataKeys;
    }
}
