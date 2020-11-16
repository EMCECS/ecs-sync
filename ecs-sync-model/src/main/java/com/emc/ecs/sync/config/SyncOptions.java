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
package com.emc.ecs.sync.config;

import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class SyncOptions {
    public static final String DB_DESC = "You must specify a DB connection string to use mySQL";

    public static final int DEFAULT_BUFFER_SIZE = 128 * 1024; // 128k
    public static final int DEFAULT_THREAD_COUNT = 16;
    public static final int DEFAULT_RETRY_ATTEMPTS = 2; // 3 total attempts
    public static final int DEFAULT_TIMING_WINDOW = 1000;

    private boolean syncMetadata = true;
    private boolean syncRetentionExpiration = false;
    private boolean syncAcl = false;
    private boolean syncData = true;

    private boolean estimationEnabled = true;

    private String sourceListFile;
    private boolean sourceListFileRawValues;

    private boolean recursive = true;
    private boolean ignoreInvalidAcls = false;

    private boolean forceSync = false;
    private boolean verify = false;
    private boolean verifyOnly = false;
    private boolean deleteSource = false;

    private int bufferSize = DEFAULT_BUFFER_SIZE;

    private int threadCount = DEFAULT_THREAD_COUNT;
    private int retryAttempts = DEFAULT_RETRY_ATTEMPTS;
    private boolean monitorPerformance = true;

    private boolean timingsEnabled = false;
    private int timingWindow = DEFAULT_TIMING_WINDOW;

    private boolean rememberFailed = false;

    private String dbFile;
    private String dbConnectString;
    private String dbEncPassword;
    private String dbTable;

    @Option(orderIndex = 10, cliInverted = true, advanced = true, description = "Metadata is synced by default")
    public boolean isSyncMetadata() {
        return syncMetadata;
    }

    public void setSyncMetadata(boolean syncMetadata) {
        this.syncMetadata = syncMetadata;
    }

    @Option(orderIndex = 20, advanced = true, description = "Sync retention/expiration information when syncing objects (in supported plugins). The target plugin will *attempt* to replicate retention/expiration for each object. Works only on plugins that support retention/expiration. If the target is an Atmos cloud, the target policy must enable retention/expiration immediately for this to work")
    public boolean isSyncRetentionExpiration() {
        return syncRetentionExpiration;
    }

    public void setSyncRetentionExpiration(boolean syncRetentionExpiration) {
        this.syncRetentionExpiration = syncRetentionExpiration;
    }

    @Option(orderIndex = 30, advanced = true, description = "Sync ACL information when syncing objects (in supported plugins)")
    public boolean isSyncAcl() {
        return syncAcl;
    }

    public void setSyncAcl(boolean syncAcl) {
        this.syncAcl = syncAcl;
    }

    @Option(orderIndex = 40, cliInverted = true, advanced = true, description = "Object data is synced by default")
    public boolean isSyncData() {
        return syncData;
    }

    public void setSyncData(boolean syncData) {
        this.syncData = syncData;
    }

    @Option(orderIndex = 45, cliInverted = true, cliName = "no-estimation", description = "By default, the source plugin will query the source storage to crawl and estimate the total amount of data to be transferred. Use this option to disable estimation (i.e. for performance improvement)")
    public boolean isEstimationEnabled() {
        return estimationEnabled;
    }

    public void setEstimationEnabled(boolean estimationEnabled) {
        this.estimationEnabled = estimationEnabled;
    }

    @Option(orderIndex = 50, description = "Path to a file that supplies the list of source objects to sync. This file must be in CSV format, with one object per line and the absolute identifier (full path or key) is the first value in each line. This entire line is available to each plugin as a raw string")
    public String getSourceListFile() {
        return sourceListFile;
    }

    public void setSourceListFile(String sourceListFile) {
        this.sourceListFile = sourceListFile;
    }

    @Option(orderIndex = 55, advanced = true, description = "Whether to treat the lines in the sourceListFile as raw values (do not do any parsing to remove comments, escapes, or trim white space). Default is false")
    public boolean isSourceListFileRawValues() {
        return sourceListFileRawValues;
    }

    public void setSourceListFileRawValues(boolean sourceListFileRawValues) {
        this.sourceListFileRawValues = sourceListFileRawValues;
    }

    @Option(orderIndex = 60, cliName = "non-recursive", cliInverted = true, advanced = true, description = "Hierarchical storage will sync recursively by default")
    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    @Option(orderIndex = 70, advanced = true, description = "If syncing ACL information when syncing objects, ignore any invalid entries (i.e. permissions or identities that don't exist in the target system)")
    public boolean isIgnoreInvalidAcls() {
        return ignoreInvalidAcls;
    }

    public void setIgnoreInvalidAcls(boolean ignoreInvalidAcls) {
        this.ignoreInvalidAcls = ignoreInvalidAcls;
    }

    @Option(orderIndex = 80, advanced = true, description = "Force the write of each object, regardless of its state in the target storage")
    public boolean isForceSync() {
        return forceSync;
    }

    public void setForceSync(boolean forceSync) {
        this.forceSync = forceSync;
    }

    @Option(orderIndex = 90, description = "After a successful object transfer, the object will be read back from the target system and its MD5 checksum will be compared with that of the source object (generated during transfer). This only compares object data (metadata is not compared) and does not include directories")
    public boolean isVerify() {
        return verify;
    }

    public void setVerify(boolean verify) {
        this.verify = verify;
    }

    @Option(orderIndex = 100, advanced = true, description = "Similar to --verify except that the object transfer is skipped and only read operations are performed (no data is written)")
    public boolean isVerifyOnly() {
        return verifyOnly;
    }

    public void setVerifyOnly(boolean verifyOnly) {
        this.verifyOnly = verifyOnly;
    }

    @Option(orderIndex = 110, advanced = true, description = "Supported source plugins will delete each source object once it is successfully synced (does not include directories). Use this option with care! Be sure log levels are appropriate to capture transferred (source deleted) objects")
    public boolean isDeleteSource() {
        return deleteSource;
    }

    public void setDeleteSource(boolean deleteSource) {
        this.deleteSource = deleteSource;
    }

    @Option(orderIndex = 120, advanced = true, description = "Sets the buffer size (in bytes) to use when streaming data from the source to the target (supported plugins only). Defaults to " + (DEFAULT_BUFFER_SIZE / 1024) + "K")
    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Option(orderIndex = 130, description = "Specifies the number of objects to sync simultaneously. Default is " + DEFAULT_THREAD_COUNT)
    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    @Option(orderIndex = 140, advanced = true, description = "Specifies how many times each object should be retried after an error. Default is 2 retries (total of 3 attempts)")
    public int getRetryAttempts() {
        return retryAttempts;
    }

    public void setRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
    }

    @Option(orderIndex = 150, cliInverted = true, advanced = true, description = "Enables performance monitoring for reads and writes on any plugin that supports it. This information is available via the REST service during a sync")
    public boolean isMonitorPerformance() {
        return monitorPerformance;
    }

    public void setMonitorPerformance(boolean monitorPerformance) {
        this.monitorPerformance = monitorPerformance;
    }

    @Option(orderIndex = 160, advanced = true, description = "Enables operation timings on all plug-ins that support it")
    public boolean isTimingsEnabled() {
        return timingsEnabled;
    }

    public void setTimingsEnabled(boolean timingsEnabled) {
        this.timingsEnabled = timingsEnabled;
    }

    @Option(orderIndex = 170, advanced = true, description = "Sets the window for timing statistics. Every {timingWindow} objects that are synced, timing statistics are logged and reset. Default is 10,000 objects")
    public int getTimingWindow() {
        return timingWindow;
    }

    public void setTimingWindow(int timingWindow) {
        this.timingWindow = timingWindow;
    }

    @Option(orderIndex = 180, advanced = true, description = "Tracks all failed objects and displays a summary of failures when finished")
    public boolean isRememberFailed() {
        return rememberFailed;
    }

    public void setRememberFailed(boolean rememberFailed) {
        this.rememberFailed = rememberFailed;
    }

    @Option(orderIndex = 200, advanced = true, description = "Enables the Sqlite database engine and specifies the file to hold the status database. A database will make repeat runs and incrementals more efficient. With this database type, you can use the sqlite3 client to interrogate the details of all objects in the sync")
    public String getDbFile() {
        return dbFile;
    }

    public void setDbFile(String dbFile) {
        this.dbFile = dbFile;
    }

    @Option(orderIndex = 210, advanced = true, description = "Enables the MySQL database engine and specifies the JDBC connect string to connect to the database (i.e. \"jdbc:mysql://localhost:3306/ecs_sync?user=foo&password=bar\"). A database will make repeat runs and incrementals more efficient. With this database type, you can use the mysql client to interrogate the details of all objects in the sync. Note that in the UI, this option is the default and is automatically populated by the server (you don't need a value here)")
    public String getDbConnectString() {
        return dbConnectString;
    }

    public void setDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
    }

    @Option(orderIndex = 215, advanced = true, description = "Specifies the encrypted password for the MySQL database")
    public String getDbEncPassword() {
        return dbEncPassword;
    }

    public void setDbEncPassword(String dbEncPassword) {
        this.dbEncPassword = dbEncPassword;
    }

    @Option(orderIndex = 220, description = "Specifies the DB table name to use. When using MySQL or the UI, be sure to provide a unique table name or risk corrupting a previously used table. Default table is \"objects\" except in the UI, where a unique name is generated for each job. In the UI, you should specify a table name to ensure the table persists after the job is archived")
    public String getDbTable() {
        return dbTable;
    }

    public void setDbTable(String dbTable) {
        this.dbTable = dbTable;
    }

    public SyncOptions withSyncMetadata(boolean syncMetadata) {
        this.syncMetadata = syncMetadata;
        return this;
    }

    public SyncOptions withSyncRetentionExpiration(boolean syncRetentionExpiration) {
        this.syncRetentionExpiration = syncRetentionExpiration;
        return this;
    }

    public SyncOptions withSyncAcl(boolean syncAcl) {
        this.syncAcl = syncAcl;
        return this;
    }

    public SyncOptions withSyncData(boolean syncData) {
        this.syncData = syncData;
        return this;
    }

    public SyncOptions withSourceListFile(String sourceListFile) {
        this.sourceListFile = sourceListFile;
        return this;
    }

    public SyncOptions withSourceListFileRawValues(boolean sourceListFileRawValues) {
        this.sourceListFileRawValues = sourceListFileRawValues;
        return this;
    }

    public SyncOptions withRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    public SyncOptions withIgnoreInvalidAcls(boolean ignoreInvalidAcls) {
        this.ignoreInvalidAcls = ignoreInvalidAcls;
        return this;
    }

    public SyncOptions withForceSync(boolean forceSync) {
        this.forceSync = forceSync;
        return this;
    }

    public SyncOptions withVerify(boolean verify) {
        this.verify = verify;
        return this;
    }

    public SyncOptions withVerifyOnly(boolean verifyOnly) {
        this.verifyOnly = verifyOnly;
        return this;
    }

    public SyncOptions withDeleteSource(boolean deleteSource) {
        this.deleteSource = deleteSource;
        return this;
    }

    public SyncOptions withBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public SyncOptions withThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    public SyncOptions withRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
        return this;
    }

    public SyncOptions withMonitorPerformance(boolean monitorPerformance) {
        this.monitorPerformance = monitorPerformance;
        return this;
    }

    public SyncOptions withTimingsEnabled(boolean timingsEnabled) {
        this.timingsEnabled = timingsEnabled;
        return this;
    }

    public SyncOptions withTimingWindow(int timingWindow) {
        this.timingWindow = timingWindow;
        return this;
    }

    public SyncOptions withRememberFailed(boolean rememberFailed) {
        this.rememberFailed = rememberFailed;
        return this;
    }

    public SyncOptions withDbFile(String dbFile) {
        this.dbFile = dbFile;
        return this;
    }

    public SyncOptions withDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
        return this;
    }

    public SyncOptions withDbTable(String dbTable) {
        this.dbTable = dbTable;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncOptions options = (SyncOptions) o;

        if (syncMetadata != options.syncMetadata) return false;
        if (syncRetentionExpiration != options.syncRetentionExpiration) return false;
        if (syncAcl != options.syncAcl) return false;
        if (syncData != options.syncData) return false;
        if (recursive != options.recursive) return false;
        if (ignoreInvalidAcls != options.ignoreInvalidAcls) return false;
        if (forceSync != options.forceSync) return false;
        if (verify != options.verify) return false;
        if (verifyOnly != options.verifyOnly) return false;
        if (deleteSource != options.deleteSource) return false;
        if (bufferSize != options.bufferSize) return false;
        if (threadCount != options.threadCount) return false;
        if (retryAttempts != options.retryAttempts) return false;
        if (monitorPerformance != options.monitorPerformance) return false;
        if (timingsEnabled != options.timingsEnabled) return false;
        if (timingWindow != options.timingWindow) return false;
        if (rememberFailed != options.rememberFailed) return false;
        if (sourceListFile != null ? !sourceListFile.equals(options.sourceListFile) : options.sourceListFile != null)
            return false;
        if (sourceListFileRawValues != options.sourceListFileRawValues) return false;
        if (dbFile != null ? !dbFile.equals(options.dbFile) : options.dbFile != null) return false;
        if (dbConnectString != null ? !dbConnectString.equals(options.dbConnectString) : options.dbConnectString != null)
            return false;
        return dbTable != null ? dbTable.equals(options.dbTable) : options.dbTable == null;
    }

    @Override
    public int hashCode() {
        int result = (syncMetadata ? 1 : 0);
        result = 31 * result + (syncRetentionExpiration ? 1 : 0);
        result = 31 * result + (syncAcl ? 1 : 0);
        result = 31 * result + (syncData ? 1 : 0);
        result = 31 * result + (sourceListFile != null ? sourceListFile.hashCode() : 0);
        result = 31 * result + (sourceListFileRawValues ? 1 : 0);
        result = 31 * result + (recursive ? 1 : 0);
        result = 31 * result + (ignoreInvalidAcls ? 1 : 0);
        result = 31 * result + (forceSync ? 1 : 0);
        result = 31 * result + (verify ? 1 : 0);
        result = 31 * result + (verifyOnly ? 1 : 0);
        result = 31 * result + (deleteSource ? 1 : 0);
        result = 31 * result + bufferSize;
        result = 31 * result + threadCount;
        result = 31 * result + retryAttempts;
        result = 31 * result + (monitorPerformance ? 1 : 0);
        result = 31 * result + (timingsEnabled ? 1 : 0);
        result = 31 * result + timingWindow;
        result = 31 * result + (rememberFailed ? 1 : 0);
        result = 31 * result + (dbFile != null ? dbFile.hashCode() : 0);
        result = 31 * result + (dbConnectString != null ? dbConnectString.hashCode() : 0);
        result = 31 * result + (dbTable != null ? dbTable.hashCode() : 0);
        return result;
    }
}
