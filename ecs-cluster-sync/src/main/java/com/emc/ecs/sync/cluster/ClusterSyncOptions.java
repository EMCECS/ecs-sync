/*
 * Copyright (c) 2024 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.cluster;

import com.emc.ecs.sync.config.ConfigurationException;

import javax.xml.bind.annotation.XmlType;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Options that control what is replicated and how.
 */
@XmlType(name = "clusterSyncOptions")
public class ClusterSyncOptions {
    public static final int DEFAULT_THREAD_COUNT = 16;
    public static final int DEFAULT_BUCKET_PARALLELISM = 4;

    public static final String DEFAULT_TARGET_NAMESPACE_SUFFIX = "_target";

    private String[] includeNamespaces;
    private String[] excludeNamespaces;
    private String targetNamespaceSuffix = DEFAULT_TARGET_NAMESPACE_SUFFIX;
    private String[] namespaceMap;
    private boolean skipNamespaceCreation;
    private boolean skipUserCreation;
    private boolean skipBucketCreation;
    private boolean dryRun;
    private boolean structureOnly;
    private String targetReplicationGroup;
    private boolean generateSourceSecretKeys = true;
    private int threadCount = DEFAULT_THREAD_COUNT;
    private int bucketParallelism = DEFAULT_BUCKET_PARALLELISM;
    private boolean verify;
    private boolean includeVersions = true;
    private String dbConnectString;
    private String dbEncPassword;
    private String dbFile;

    public String[] getIncludeNamespaces() {
        return includeNamespaces;
    }

    public void setIncludeNamespaces(String[] includeNamespaces) {
        this.includeNamespaces = includeNamespaces;
    }

    public String[] getExcludeNamespaces() {
        return excludeNamespaces;
    }

    public void setExcludeNamespaces(String[] excludeNamespaces) {
        this.excludeNamespaces = excludeNamespaces;
    }

    /**
     * Suffix appended to a namespace name when that name is already in use on the target cluster. A counter is added
     * if the suffixed name is also taken. Object users in a renamed namespace get the same suffix.
     */
    public String getTargetNamespaceSuffix() {
        return targetNamespaceSuffix;
    }

    public void setTargetNamespaceSuffix(String targetNamespaceSuffix) {
        this.targetNamespaceSuffix = targetNamespaceSuffix;
    }

    /**
     * Explicit {@code source=target} namespace names, which bypass conflict detection. Use this to make a re-run
     * deterministic: without it, a namespace created by a previous run is seen as a conflict and a new name is
     * allocated instead of resuming into the existing one.
     */
    public String[] getNamespaceMap() {
        return namespaceMap;
    }

    public void setNamespaceMap(String[] namespaceMap) {
        this.namespaceMap = namespaceMap;
    }

    /**
     * Parses {@link #getNamespaceMap()} into source-to-target pairs.
     */
    public Map<String, String> parseNamespaceMap() {
        Map<String, String> parsed = new LinkedHashMap<>();
        if (namespaceMap == null) return parsed;
        for (String entry : namespaceMap) {
            if (entry == null || entry.trim().isEmpty()) continue;
            int separator = entry.indexOf('=');
            if (separator < 1 || separator == entry.length() - 1)
                throw new ConfigurationException("namespace mapping must be in the form source=target, got: " + entry);
            parsed.put(entry.substring(0, separator).trim(), entry.substring(separator + 1).trim());
        }
        return parsed;
    }

    public boolean isSkipNamespaceCreation() {
        return skipNamespaceCreation;
    }

    public void setSkipNamespaceCreation(boolean skipNamespaceCreation) {
        this.skipNamespaceCreation = skipNamespaceCreation;
    }

    public boolean isSkipUserCreation() {
        return skipUserCreation;
    }

    public void setSkipUserCreation(boolean skipUserCreation) {
        this.skipUserCreation = skipUserCreation;
    }

    public boolean isSkipBucketCreation() {
        return skipBucketCreation;
    }

    public void setSkipBucketCreation(boolean skipBucketCreation) {
        this.skipBucketCreation = skipBucketCreation;
    }

    /**
     * Discover and report the replication plan without modifying either cluster.
     */
    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    /**
     * Create namespaces, users and buckets but do not copy any objects.
     */
    public boolean isStructureOnly() {
        return structureOnly;
    }

    public void setStructureOnly(boolean structureOnly) {
        this.structureOnly = structureOnly;
    }

    /**
     * Name of the replication group to use on the target cluster. When omitted, the source bucket's replication group
     * name is matched by name on the target; if no match exists, the target's only replication group is used.
     */
    public String getTargetReplicationGroup() {
        return targetReplicationGroup;
    }

    public void setTargetReplicationGroup(String targetReplicationGroup) {
        this.targetReplicationGroup = targetReplicationGroup;
    }

    /**
     * The management API does not expose the secret key of an existing user. When enabled (the default), a secret key
     * is generated on the source cluster for each user that has none, so that the data can be read. This is a
     * modification of the source cluster; disable to fail instead.
     */
    public boolean isGenerateSourceSecretKeys() {
        return generateSourceSecretKeys;
    }

    public void setGenerateSourceSecretKeys(boolean generateSourceSecretKeys) {
        this.generateSourceSecretKeys = generateSourceSecretKeys;
    }

    /**
     * Threads used within each individual bucket copy job.
     */
    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    /**
     * Number of bucket copy jobs to run concurrently.
     */
    public int getBucketParallelism() {
        return bucketParallelism;
    }

    public void setBucketParallelism(int bucketParallelism) {
        this.bucketParallelism = bucketParallelism;
    }

    public boolean isVerify() {
        return verify;
    }

    public void setVerify(boolean verify) {
        this.verify = verify;
    }

    /**
     * Copy all versions of objects in versioned buckets. Requires versioning to be enabled on the target bucket.
     */
    public boolean isIncludeVersions() {
        return includeVersions;
    }

    public void setIncludeVersions(boolean includeVersions) {
        this.includeVersions = includeVersions;
    }

    public String getDbConnectString() {
        return dbConnectString;
    }

    public void setDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
    }

    public String getDbEncPassword() {
        return dbEncPassword;
    }

    public void setDbEncPassword(String dbEncPassword) {
        this.dbEncPassword = dbEncPassword;
    }

    public String getDbFile() {
        return dbFile;
    }

    public void setDbFile(String dbFile) {
        this.dbFile = dbFile;
    }
}
