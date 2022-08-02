/*
 * Copyright (c) 2016-2018 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config;

import javax.xml.bind.annotation.*;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@XmlRootElement
@XmlType(propOrder = {"jobName", "options", "sourceWrapper", "filters", "targetWrapper", "properties"})
public class SyncConfig {
    private String jobName;
    private Object source;
    private Object target;
    private List<?> filters;
    private SyncOptions options = new SyncOptions();
    private Map<String, String> properties = new TreeMap<>();

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @XmlTransient
    public Object getSource() {
        return source;
    }

    public void setSource(Object source) {
        this.source = source;
    }

    @XmlElement(name = "source")
    public StorageWrapper getSourceWrapper() {
        if (source == null) return null;
        return new StorageWrapper(source);
    }

    public void setSourceWrapper(StorageWrapper sourceWrapper) {
        if (sourceWrapper == null) this.source = null;
        else this.source = sourceWrapper.config;
    }

    @XmlTransient
    public Object getTarget() {
        return target;
    }

    public void setTarget(Object target) {
        this.target = target;
    }

    @XmlElement(name = "target")
    public StorageWrapper getTargetWrapper() {
        if (target == null) return null;
        return new StorageWrapper(target);
    }

    public void setTargetWrapper(StorageWrapper targetWrapper) {
        if (targetWrapper == null) this.target = null;
        else this.target = targetWrapper.config;
    }

    @XmlElementWrapper(name = "filters")
    @XmlAnyElement(lax = true)
    public List<?> getFilters() {
        return filters;
    }

    public void setFilters(List<?> filters) {
        this.filters = filters;
    }

    public SyncOptions getOptions() {
        return options;
    }

    public void setOptions(SyncOptions options) {
        this.options = options;
    }

    @XmlJavaTypeAdapter(MapAdapter.class)
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public SyncConfig withSource(Object source) {
        setSource(source);
        return this;
    }

    public SyncConfig withTarget(Object target) {
        setTarget(target);
        return this;
    }

    public SyncConfig withFilters(List<?> filters) {
        setFilters(filters);
        return this;
    }

    public SyncConfig withOptions(SyncOptions options) {
        setOptions(options);
        return this;
    }

    public SyncConfig withProperties(Map<String, String> properties) {
        setProperties(properties);
        return this;
    }

    public SyncConfig withProperty(String name, String value) {
        getProperties().put(name, value);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncConfig that = (SyncConfig) o;

        if (jobName != null ? !jobName.equals(that.jobName) : that.jobName != null) return false;
        if (source != null ? !source.equals(that.source) : that.source != null) return false;
        if (target != null ? !target.equals(that.target) : that.target != null) return false;
        if (filters != null ? !filters.equals(that.filters) : that.filters != null) return false;
        if (options != null ? !options.equals(that.options) : that.options != null) return false;
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = jobName != null ? jobName.hashCode() : 0;
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (target != null ? target.hashCode() : 0);
        result = 31 * result + (filters != null ? filters.hashCode() : 0);
        result = 31 * result + (options != null ? options.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }

    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    public static class StorageWrapper {
        @XmlAnyElement(lax = true)
        public Object config;

        public StorageWrapper() {
        }

        StorageWrapper(Object config) {
            this.config = config;
        }
    }
}
