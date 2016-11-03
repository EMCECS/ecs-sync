package com.emc.ecs.sync.config;

import javax.xml.bind.annotation.*;
import java.util.List;

@XmlRootElement
@XmlType(propOrder = {"options", "sourceWrapper", "filters", "targetWrapper"})
public class SyncConfig {
    private Object source;
    private Object target;
    private List<?> filters;
    private SyncOptions options = new SyncOptions();

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncConfig that = (SyncConfig) o;

        if (source != null ? !source.equals(that.source) : that.source != null) return false;
        if (target != null ? !target.equals(that.target) : that.target != null) return false;
        if (filters != null ? !filters.equals(that.filters) : that.filters != null) return false;
        return options != null ? options.equals(that.options) : that.options == null;
    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 31 * result + (target != null ? target.hashCode() : 0);
        result = 31 * result + (filters != null ? filters.hashCode() : 0);
        result = 31 * result + (options != null ? options.hashCode() : 0);
        return result;
    }

    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    private static class StorageWrapper {
        @XmlAnyElement(lax = true)
        public Object config;

        public StorageWrapper() {
        }

        StorageWrapper(Object config) {
            this.config = config;
        }
    }
}
