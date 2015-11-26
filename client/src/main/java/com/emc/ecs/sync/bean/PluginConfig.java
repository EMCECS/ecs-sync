/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.bean;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.HashMap;
import java.util.Map;

public class PluginConfig {
    protected Class pluginClass;
    protected Map<String, String> customProperties = new HashMap<>();

    public PluginConfig() {
    }

    public PluginConfig(Class pluginClass) {
        this.pluginClass = pluginClass;
    }

    @XmlAttribute(name = "Class")
    public Class getPluginClass() {
        return pluginClass;
    }

    public void setPluginClass(Class pluginClass) {
        this.pluginClass = pluginClass;
    }

    @XmlElement(name = "Property")
    public PropEntry[] getPropEntries() {
        PropEntry[] entries = new PropEntry[customProperties.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            entries[i++] = new PropEntry(entry.getKey(), entry.getValue());
        }
        return entries;
    }

    public void setPropEntries(PropEntry[] propEntries) {
        customProperties = new HashMap<>();
        for (PropEntry entry : propEntries) {
            customProperties.put(entry.getName(), entry.getValue());
        }
    }

    @XmlTransient
    public Map<String, String> getCustomProperties() {
        return customProperties;
    }

    public void setCustomProperties(Map<String, String> customProperties) {
        this.customProperties = customProperties;
    }

    public PluginConfig withPluginClass(Class pluginClass) {
        setPluginClass(pluginClass);
        return this;
    }

    public PluginConfig addCustomProperty(String name, String value) {
        customProperties.put(name, value);
        return this;
    }
}
