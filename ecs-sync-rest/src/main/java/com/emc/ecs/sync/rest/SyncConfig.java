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
package com.emc.ecs.sync.rest;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "SyncConfig")
@XmlType(propOrder = {"name", "source", "target", "filters", "queryThreadCount", "syncThreadCount", "recursive",
        "timingsEnabled", "timingWindow", "rememberFailed", "verify", "verifyOnly", "deleteSource", "logLevel",
        "dbFile", "dbConnectString", "dbTable", "splitPoolsThreshold", "metadataOnly", "ignoreMetadata",
        "includeAcl", "ignoreInvalidAcls", "includeRetentionExpiration", "force", "bufferSize", "monitorPerformance"})
public class SyncConfig {

    // MAIN CONFIGURATION PROPERTIES
    protected String name;
    protected PluginConfig source;
    protected PluginConfig target;
    protected List<PluginConfig> filters = new ArrayList<>();
    protected Integer queryThreadCount;
    protected Integer syncThreadCount;
    protected Boolean recursive;
    protected Boolean timingsEnabled;
    protected Integer timingWindow;
    protected Boolean rememberFailed;
    protected Boolean verify;
    protected Boolean verifyOnly;
    protected Boolean deleteSource;
    protected String logLevel;
    protected String dbFile;
    protected String dbConnectString;
    protected String dbTable;
    protected Integer splitPoolsThreshold;

    // COMMON PLUGIN PROPERTIES
    protected Boolean metadataOnly;
    protected Boolean ignoreMetadata;
    protected Boolean includeAcl;
    protected Boolean ignoreInvalidAcls;
    protected Boolean includeRetentionExpiration;
    protected Boolean force;
    protected Integer bufferSize;
    protected Boolean monitorPerformance;

    @XmlElement(name = "Name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @XmlElement(name = "Source")
    public PluginConfig getSource() {
        return source;
    }

    public void setSource(PluginConfig source) {
        this.source = source;
    }

    @XmlElement(name = "Target")
    public PluginConfig getTarget() {
        return target;
    }

    public void setTarget(PluginConfig target) {
        this.target = target;
    }

    @XmlElement(name = "Filter")
    public List<PluginConfig> getFilters() {
        return filters;
    }

    public void setFilters(List<PluginConfig> filters) {
        this.filters = filters;
    }

    @XmlElement(name = "QueryThreadCount")
    public Integer getQueryThreadCount() {
        return queryThreadCount;
    }

    public void setQueryThreadCount(Integer queryThreadCount) {
        this.queryThreadCount = queryThreadCount;
    }

    @XmlElement(name = "SyncThreadCount")
    public Integer getSyncThreadCount() {
        return syncThreadCount;
    }

    public void setSyncThreadCount(Integer syncThreadCount) {
        this.syncThreadCount = syncThreadCount;
    }

    @XmlElement(name = "Recursive")
    public Boolean getRecursive() {
        return recursive;
    }

    public void setRecursive(Boolean recursive) {
        this.recursive = recursive;
    }

    @XmlElement(name = "TimingsEnabled")
    public Boolean getTimingsEnabled() {
        return timingsEnabled;
    }

    public void setTimingsEnabled(Boolean timingsEnabled) {
        this.timingsEnabled = timingsEnabled;
    }

    @XmlElement(name = "TimingWindow")
    public Integer getTimingWindow() {
        return timingWindow;
    }

    public void setTimingWindow(Integer timingWindow) {
        this.timingWindow = timingWindow;
    }

    @XmlElement(name = "RememberFailed")
    public Boolean getRememberFailed() {
        return rememberFailed;
    }

    public void setRememberFailed(Boolean rememberFailed) {
        this.rememberFailed = rememberFailed;
    }

    @XmlElement(name = "Verify")
    public Boolean getVerify() {
        return verify;
    }

    public void setVerify(Boolean verify) {
        this.verify = verify;
    }

    @XmlElement(name = "VerifyOnly")
    public Boolean getVerifyOnly() {
        return verifyOnly;
    }

    public void setVerifyOnly(Boolean verifyOnly) {
        this.verifyOnly = verifyOnly;
    }

    @XmlElement(name = "DeleteSource")
    public Boolean getDeleteSource() {
        return deleteSource;
    }

    public void setDeleteSource(Boolean deleteSource) {
        this.deleteSource = deleteSource;
    }

    @XmlElement(name = "LogLevel")
    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    @XmlElement(name = "DbFile")
    public String getDbFile() {
        return dbFile;
    }

    public void setDbFile(String dbFile) {
        this.dbFile = dbFile;
    }

    @XmlElement(name = "DbConnectString")
    public String getDbConnectString() {
        return dbConnectString;
    }

    public void setDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
    }

    @XmlElement(name = "DbTable")
    public String getDbTable() {
        return dbTable;
    }

    public void setDbTable(String dbTable) {
        this.dbTable = dbTable;
    }

    @XmlElement(name = "SplitPoolsThreshold")
    public Integer getSplitPoolsThreshold() {
        return splitPoolsThreshold;
    }

    public void setSplitPoolsThreshold(Integer splitPoolsThreshold) {
        this.splitPoolsThreshold = splitPoolsThreshold;
    }

    @XmlElement(name = "MetadataOnly")
    public Boolean getMetadataOnly() {
        return metadataOnly;
    }

    public void setMetadataOnly(Boolean metadataOnly) {
        this.metadataOnly = metadataOnly;
    }

    @XmlElement(name = "IgnoreMetadata")
    public Boolean getIgnoreMetadata() {
        return ignoreMetadata;
    }

    public void setIgnoreMetadata(Boolean ignoreMetadata) {
        this.ignoreMetadata = ignoreMetadata;
    }

    @XmlElement(name = "IncludeAcl")
    public Boolean getIncludeAcl() {
        return includeAcl;
    }

    public void setIncludeAcl(Boolean includeAcl) {
        this.includeAcl = includeAcl;
    }

    @XmlElement(name = "IgnoreInvalidAcls")
    public Boolean getIgnoreInvalidAcls() {
        return ignoreInvalidAcls;
    }

    public void setIgnoreInvalidAcls(Boolean ignoreInvalidAcls) {
        this.ignoreInvalidAcls = ignoreInvalidAcls;
    }

    @XmlElement(name = "IncludeRetentionExpiration")
    public Boolean getIncludeRetentionExpiration() {
        return includeRetentionExpiration;
    }

    public void setIncludeRetentionExpiration(Boolean includeRetentionExpiration) {
        this.includeRetentionExpiration = includeRetentionExpiration;
    }

    @XmlElement(name = "Force")
    public Boolean getForce() {
        return force;
    }

    public void setForce(Boolean force) {
        this.force = force;
    }

    @XmlElement(name = "BufferSize")
    public Integer getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
    }

    @XmlElement(name = "MonitorPerformance")
    public Boolean getMonitorPerformance() {
        return monitorPerformance;
    }

    public void setMonitorPerformance(Boolean monitorPerformance) {
        this.monitorPerformance = monitorPerformance;
    }
}
