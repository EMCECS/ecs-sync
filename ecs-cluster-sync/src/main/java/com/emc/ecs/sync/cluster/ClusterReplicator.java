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

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.object.s3.S3Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Replicates an entire ECS cluster to another cluster.
 * <p>
 * Runs in three ordered phases:
 * <ol>
 *     <li>discover the source tenancy structure</li>
 *     <li>create namespaces, then object users, then buckets (owned by the replicated user) on the target</li>
 *     <li>copy the objects of every bucket, using one {@link EcsSync} job per bucket</li>
 * </ol>
 */
public class ClusterReplicator implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ClusterReplicator.class);

    private final ClusterSyncConfig config;
    private final ClusterSyncOptions options;
    private final EcsManagementClient sourceClient;
    private final EcsManagementClient targetClient;
    // buckets are copied concurrently, so the credential caches must be thread-safe
    private final Map<String, String> sourceKeyCache = new ConcurrentHashMap<>();
    private final Map<String, String> targetKeyCache = new ConcurrentHashMap<>();
    private final ClusterSyncReport report = new ClusterSyncReport();

    public ClusterReplicator(ClusterSyncConfig config) {
        config.validate();
        this.config = config;
        this.options = config.getOptions();
        this.sourceClient = new EcsManagementClient(config.getSource(), "source");
        try {
            this.targetClient = new EcsManagementClient(config.getTarget(), "target");
        } catch (RuntimeException e) {
            sourceClient.close();
            throw e;
        }
    }

    public ClusterSyncReport run() {
        if (config.isSameCluster()) {
            log.info("migrating within cluster {} into replication group {}",
                    config.getSource().getManagementEndpoint(), options.getTargetReplicationGroup());
        } else {
            log.info("replicating cluster {} -> {}",
                    config.getSource().getManagementEndpoint(), config.getTarget().getManagementEndpoint());
        }

        ClusterReplicationPlan plan = discover();
        report.setPlan(plan);

        // resolve target names and the target replication group before anything is created, so that a dry run
        // surfaces exactly what a real run would do (including a bad replication group name)
        ReplicationGroupResolver groupResolver = new ReplicationGroupResolver(
                targetClient.listReplicationGroups(), options.getTargetReplicationGroup());
        Map<String, NamespaceMapper.Mapping> mappings = resolveNamespaceNames(plan);
        report.setNamespaceMappings(mappings);

        if (options.isDryRun()) {
            log.info("dry run: no changes will be made to either cluster");
            logPlan(plan, mappings, groupResolver);
            return report;
        }

        replicateStructure(plan, mappings, groupResolver);

        if (options.isStructureOnly()) {
            log.info("structure-only: skipping object copy");
            return report;
        }

        copyAllBuckets(plan, mappings);
        return report;
    }

    /**
     * Works out the target namespace name for every source namespace up front.
     */
    private Map<String, NamespaceMapper.Mapping> resolveNamespaceNames(ClusterReplicationPlan plan) {
        Set<String> existing = new HashSet<>();
        targetClient.listNamespaces().forEach(ns -> existing.add(ns.getName()));

        NamespaceMapper mapper = new NamespaceMapper(
                options.getTargetNamespaceSuffix(), options.parseNamespaceMap(), existing);

        Map<String, NamespaceMapper.Mapping> mappings = new LinkedHashMap<>();
        for (EcsNamespace namespace : plan.getNamespaces()) {
            mappings.put(namespace.getName(), mapper.map(namespace.getName()));
        }
        return mappings;
    }

    // ---------------------------------------------------------------- phase 1: discovery

    private ClusterReplicationPlan discover() {
        log.info("phase 1/3: discovering source cluster structure");

        List<EcsNamespace> allNamespaces = sourceClient.listNamespaces();
        NamespaceFilter filter = new NamespaceFilter(options.getIncludeNamespaces(), options.getExcludeNamespaces());
        Set<String> missing = filter.unmatchedIncludes(allNamespaces);
        if (!missing.isEmpty()) {
            log.warn("these namespaces were requested but do not exist on the source cluster: {}", missing);
        }
        List<EcsNamespace> namespaces = filter.apply(allNamespaces);

        // replication group IDs are cluster-specific; record the name so it can be matched on the target
        Map<String, String> sourceGroupNames = new HashMap<>();
        for (ReplicationGroup group : sourceClient.listReplicationGroups()) {
            sourceGroupNames.put(group.getId(), group.getName());
        }

        ClusterReplicationPlan plan = new ClusterReplicationPlan();
        for (EcsNamespace namespace : namespaces) {
            List<EcsObjectUser> users = sourceClient.listObjectUsers(namespace.getName());
            List<EcsBucketInfo> buckets = sourceClient.listBuckets(namespace.getName());
            for (EcsBucketInfo bucket : buckets) {
                bucket.setReplicationGroupName(sourceGroupNames.get(bucket.getReplicationGroupId()));
            }
            plan.addNamespace(namespace, users, buckets);
            log.info("namespace {}: {} users, {} buckets", namespace.getName(), users.size(), buckets.size());
        }

        log.info("discovered {} namespaces, {} users, {} buckets",
                plan.getNamespaces().size(), plan.getUserCount(), plan.getBucketCount());
        return plan;
    }

    private void logPlan(ClusterReplicationPlan plan, Map<String, NamespaceMapper.Mapping> mappings,
                         ReplicationGroupResolver groupResolver) {
        for (EcsNamespace namespace : plan.getNamespaces()) {
            NamespaceMapper.Mapping mapping = mappings.get(namespace.getName());
            if (mapping.isRenamed()) {
                log.info("would create namespace {} as {} ({} already exists on the target)",
                        namespace.getName(), mapping.getTargetNamespace(), namespace.getName());
            } else {
                log.info("would create namespace {}", mapping.getTargetNamespace());
            }
            for (EcsObjectUser user : plan.getUsers(namespace.getName())) {
                String targetUser = mapping.mapUserId(user.getUserId());
                log.info("  would create user {}{}", targetUser,
                        targetUser.equals(user.getUserId()) ? "" : " (renamed from " + user.getUserId() + ")");
            }
            for (EcsBucketInfo bucket : plan.getBuckets(namespace.getName())) {
                String owner = mapping.mapUserId(plan.resolveBucketUser(bucket));
                log.info("  would create bucket {} in {} (owner={}, replication-group={}) and copy its objects",
                        bucket.getName(), mapping.getTargetNamespace(), owner,
                        describeGroup(bucket, groupResolver));
            }
        }
    }

    private String describeGroup(EcsBucketInfo bucket, ReplicationGroupResolver groupResolver) {
        try {
            return groupResolver.resolve(bucket.getReplicationGroupName());
        } catch (RuntimeException e) {
            return "UNRESOLVED: " + e.getMessage();
        }
    }

    // ---------------------------------------------------------------- phase 2: structure

    private void replicateStructure(ClusterReplicationPlan plan, Map<String, NamespaceMapper.Mapping> mappings,
                                   ReplicationGroupResolver groupResolver) {
        log.info("phase 2/3: creating namespaces, users and buckets on the target");

        for (EcsNamespace namespace : plan.getNamespaces()) {
            String sourceNamespace = namespace.getName();
            NamespaceMapper.Mapping mapping = mappings.get(sourceNamespace);
            String targetNamespace = mapping.getTargetNamespace();

            // 1. namespace
            if (!options.isSkipNamespaceCreation()) {
                if (targetClient.getNamespace(targetNamespace) != null) {
                    log.info("namespace {} already exists on the target", targetNamespace);
                } else {
                    targetClient.createNamespace(targetNamespace, groupResolver.resolve(null));
                    report.recordNamespaceCreated();
                }
            }

            // 2. users - must exist before buckets so that buckets can be owned by them
            if (!options.isSkipUserCreation()) {
                List<String> existing = new ArrayList<>();
                targetClient.listObjectUsers(targetNamespace).forEach(u -> existing.add(u.getUserId()));
                for (EcsObjectUser user : plan.getUsers(sourceNamespace)) {
                    String targetUserId = mapping.mapUserId(user.getUserId());
                    if (existing.contains(targetUserId)) {
                        log.info("user {} already exists in namespace {} on the target",
                                targetUserId, targetNamespace);
                    } else {
                        targetClient.createObjectUser(targetUserId, targetNamespace);
                        report.recordUserCreated();
                    }
                }
            }

            // 3. buckets, owned by the replicated user
            if (!options.isSkipBucketCreation()) {
                for (EcsBucketInfo bucket : plan.getBuckets(sourceNamespace)) {
                    String owner = mapping.mapUserId(plan.resolveBucketUser(bucket));
                    if (targetClient.getBucketInfo(bucket.getName(), targetNamespace) != null) {
                        log.info("bucket {} already exists in namespace {} on the target",
                                bucket.getName(), targetNamespace);
                        continue;
                    }
                    targetClient.createBucket(bucket.getName(), targetNamespace,
                            groupResolver.resolve(bucket.getReplicationGroupName()), owner,
                            bucket.isFileSystemEnabled());
                    report.recordBucketCreated();
                }
            }
        }
    }

    // ---------------------------------------------------------------- phase 3: data

    private void copyAllBuckets(ClusterReplicationPlan plan, Map<String, NamespaceMapper.Mapping> mappings) {
        List<EcsBucketInfo> buckets = plan.getAllBuckets();
        log.info("phase 3/3: copying objects for {} buckets ({} at a time, {} threads each)",
                buckets.size(), options.getBucketParallelism(), options.getThreadCount());

        ExecutorService executor = Executors.newFixedThreadPool(options.getBucketParallelism());
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (EcsBucketInfo bucket : buckets) {
                futures.add(executor.submit(
                        () -> copyBucketGuarded(plan, bucket, mappings.get(bucket.getNamespace()))));
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    // individual failures are already recorded; keep going
                    log.debug("bucket copy task ended with an exception", e);
                }
            }
        } finally {
            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        log.info("object copy complete: {} buckets succeeded, {} failed, {} skipped",
                report.getBucketsCopied(), report.getBucketsFailed(), report.getBucketsSkipped());
    }

    private void copyBucketGuarded(ClusterReplicationPlan plan, EcsBucketInfo bucket,
                                   NamespaceMapper.Mapping mapping) {
        String label = bucket.getNamespace() + "/" + bucket.getName();
        try {
            copyBucket(plan, bucket, mapping);
        } catch (Exception e) {
            log.error("failed to copy bucket {}: {}", label, e.toString());
            log.debug("stacktrace for {}", label, e);
            report.recordBucketFailed(label, e.toString());
        }
    }

    private void copyBucket(ClusterReplicationPlan plan, EcsBucketInfo bucket, NamespaceMapper.Mapping mapping) {
        String sourceNamespace = bucket.getNamespace();
        String targetNamespace = mapping.getTargetNamespace();
        String label = sourceNamespace + "/" + bucket.getName();
        String sourceUserId = plan.resolveBucketUser(bucket);
        if (sourceUserId == null) {
            log.warn("skipping bucket {}: namespace {} has no object users, so there is no identity to read or "
                    + "write its data with", label, sourceNamespace);
            report.recordBucketSkipped(label, "no object user in namespace");
            return;
        }
        String targetUserId = mapping.mapUserId(sourceUserId);

        // credentials must be obtained separately per cluster and per namespace - a user's key on the source is
        // unrelated to the key of the corresponding user on the target
        String sourceSecret = sourceSecretKey(sourceUserId, sourceNamespace);
        String targetSecret = targetSecretKey(targetUserId, targetNamespace);

        boolean copyVersions = reconcileVersioning(bucket, sourceNamespace, sourceUserId, sourceSecret,
                targetNamespace, targetUserId, targetSecret);

        EcsS3Config source = new EcsS3Config();
        S3Support.applyEndpoint(config.getSource(), source, sourceNamespace);
        source.setAccessKey(sourceUserId);
        source.setSecretKey(sourceSecret);
        source.setBucketName(bucket.getName());
        source.setIncludeVersions(copyVersions);

        EcsS3Config target = new EcsS3Config();
        // the namespace is pinned explicitly: for a same-cluster migration the source and target buckets have the
        // same name, so without it both sides could resolve to the same namespace
        S3Support.applyEndpoint(config.getTarget(), target, targetNamespace);
        target.setAccessKey(targetUserId);
        target.setSecretKey(targetSecret);
        target.setBucketName(bucket.getName());
        target.setIncludeVersions(copyVersions);
        target.setCreateBucket(false); // created in phase 2, with the correct owner and replication group

        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(source);
        syncConfig.setTarget(target);
        syncConfig.setOptions(buildSyncOptions(bucket, targetNamespace));

        log.info("copying {}/{} -> {}/{} as {}{}", sourceNamespace, bucket.getName(), targetNamespace,
                bucket.getName(), targetUserId, copyVersions ? " (including versions)" : "");
        try (EcsSync sync = new EcsSync()) {
            sync.setSyncConfig(syncConfig);
            sync.run();

            long failed = sync.getStats().getObjectsFailed();
            long complete = sync.getStats().getObjectsComplete();
            if (failed > 0) {
                throw new RuntimeException(String.format(
                        "%d of %d objects failed to copy", failed, failed + complete));
            }
            log.info("copied bucket {}: {} objects, {} bytes", label, complete, sync.getStats().getBytesComplete());
            report.recordBucketCopied(complete, sync.getStats().getBytesComplete());
        }
    }

    /**
     * Ensures the target bucket's versioning state matches the source, so that versioned copies are possible.
     *
     * @return true if all versions should be copied
     */
    private boolean reconcileVersioning(EcsBucketInfo bucket, String sourceNamespace, String sourceUserId,
                                        String sourceSecret, String targetNamespace, String targetUserId,
                                        String targetSecret) {
        if (!options.isIncludeVersions()) return false;

        S3Client sourceS3 = null;
        S3Client targetS3 = null;
        try {
            sourceS3 = S3Support.createClient(config.getSource(), sourceNamespace, sourceUserId, sourceSecret);
            if (!S3Support.isVersioningEnabled(sourceS3, bucket.getName())) return false;

            targetS3 = S3Support.createClient(config.getTarget(), targetNamespace, targetUserId, targetSecret);
            if (!S3Support.isVersioningEnabled(targetS3, bucket.getName())) {
                S3Support.enableVersioning(targetS3, bucket.getName());
            }
            return true;
        } catch (Exception e) {
            log.warn("could not reconcile versioning for bucket {}/{} ({}); copying current versions only",
                    bucket.getNamespace(), bucket.getName(), e.toString());
            return false;
        } finally {
            destroyQuietly(sourceS3);
            destroyQuietly(targetS3);
        }
    }

    private static void destroyQuietly(S3Client s3) {
        if (s3 == null) return;
        try {
            s3.destroy();
        } catch (Exception e) {
            log.debug("could not destroy S3 client", e);
        }
    }

    private SyncOptions buildSyncOptions(EcsBucketInfo bucket, String targetNamespace) {
        SyncOptions syncOptions = new SyncOptions();
        syncOptions.setThreadCount(options.getThreadCount());
        syncOptions.setVerify(options.isVerify());
        syncOptions.setSyncMetadata(true);
        syncOptions.setSyncAcl(true);
        if (options.getDbConnectString() != null) {
            syncOptions.setDbConnectString(options.getDbConnectString());
            syncOptions.setDbEncPassword(options.getDbEncPassword());
            syncOptions.setDbTable(dbTableName(bucket, targetNamespace));
        } else if (options.getDbFile() != null) {
            syncOptions.setDbFile(options.getDbFile());
            syncOptions.setDbTable(dbTableName(bucket, targetNamespace));
        }
        return syncOptions;
    }

    /**
     * The table is keyed on the target namespace so that two runs copying the same source bucket into different
     * target namespaces do not share (and corrupt) each other's status.
     */
    static String dbTableName(EcsBucketInfo bucket, String targetNamespace) {
        String raw = "cluster_sync_" + targetNamespace + "_" + bucket.getName();
        String sanitized = raw.replaceAll("[^A-Za-z0-9_]", "_");
        // MySQL identifiers are limited to 64 characters
        if (sanitized.length() > 64) {
            String suffix = Integer.toHexString(raw.hashCode());
            sanitized = sanitized.substring(0, 64 - suffix.length() - 1) + "_" + suffix;
        }
        return sanitized;
    }

    private String sourceSecretKey(String userId, String namespace) {
        return sourceKeyCache.computeIfAbsent(namespace + "/" + userId,
                k -> sourceClient.getOrCreateSecretKey(userId, namespace, options.isGenerateSourceSecretKeys()));
    }

    private String targetSecretKey(String userId, String namespace) {
        return targetKeyCache.computeIfAbsent(namespace + "/" + userId,
                k -> targetClient.getOrCreateSecretKey(userId, namespace, true));
    }

    @Override
    public void close() {
        sourceClient.close();
        targetClient.close();
    }
}
