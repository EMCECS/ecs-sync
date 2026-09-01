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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Outcome of a cluster replication. Updated from multiple bucket-copy threads.
 */
public class ClusterSyncReport {
    private final AtomicInteger namespacesCreated = new AtomicInteger();
    private final AtomicInteger usersCreated = new AtomicInteger();
    private final AtomicInteger bucketsCreated = new AtomicInteger();
    private final AtomicInteger bucketsCopied = new AtomicInteger();
    private final AtomicLong objectsCopied = new AtomicLong();
    private final AtomicLong bytesCopied = new AtomicLong();
    private final Map<String, String> failures = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, String> skipped = Collections.synchronizedMap(new LinkedHashMap<>());
    private ClusterReplicationPlan plan;
    private Map<String, NamespaceMapper.Mapping> namespaceMappings = Collections.emptyMap();

    void recordNamespaceCreated() {
        namespacesCreated.incrementAndGet();
    }

    void recordUserCreated() {
        usersCreated.incrementAndGet();
    }

    void recordBucketCreated() {
        bucketsCreated.incrementAndGet();
    }

    void recordBucketCopied(long objects, long bytes) {
        bucketsCopied.incrementAndGet();
        objectsCopied.addAndGet(objects);
        bytesCopied.addAndGet(bytes);
    }

    void recordBucketFailed(String bucket, String reason) {
        failures.put(bucket, reason);
    }

    void recordBucketSkipped(String bucket, String reason) {
        skipped.put(bucket, reason);
    }

    void setPlan(ClusterReplicationPlan plan) {
        this.plan = plan;
    }

    public ClusterReplicationPlan getPlan() {
        return plan;
    }

    void setNamespaceMappings(Map<String, NamespaceMapper.Mapping> namespaceMappings) {
        this.namespaceMappings = namespaceMappings;
    }

    /**
     * Source namespace name -> the naming decision made for the target.
     */
    public Map<String, NamespaceMapper.Mapping> getNamespaceMappings() {
        return Collections.unmodifiableMap(namespaceMappings);
    }

    public int getNamespacesCreated() {
        return namespacesCreated.get();
    }

    public int getUsersCreated() {
        return usersCreated.get();
    }

    public int getBucketsCreated() {
        return bucketsCreated.get();
    }

    public int getBucketsCopied() {
        return bucketsCopied.get();
    }

    public int getBucketsFailed() {
        return failures.size();
    }

    public int getBucketsSkipped() {
        return skipped.size();
    }

    public long getObjectsCopied() {
        return objectsCopied.get();
    }

    public long getBytesCopied() {
        return bytesCopied.get();
    }

    public Map<String, String> getFailures() {
        return Collections.unmodifiableMap(failures);
    }

    public Map<String, String> getSkipped() {
        return Collections.unmodifiableMap(skipped);
    }

    public boolean isSuccessful() {
        return failures.isEmpty();
    }

    public String summarize() {
        StringBuilder builder = new StringBuilder();
        List<NamespaceMapper.Mapping> renamed = namespaceMappings.values().stream()
                .filter(NamespaceMapper.Mapping::isRenamed).collect(Collectors.toList());
        if (!renamed.isEmpty()) {
            builder.append("renamed namespaces:");
            renamed.forEach(m -> builder.append("\n  ").append(m.getSourceNamespace())
                    .append(" -> ").append(m.getTargetNamespace())
                    .append("   (users suffixed with ").append(m.getAppliedSuffix()).append(")"));
            builder.append('\n');
        }
        builder.append("namespaces created: ").append(getNamespacesCreated())
                .append("\nusers created:      ").append(getUsersCreated())
                .append("\nbuckets created:    ").append(getBucketsCreated())
                .append("\nbuckets copied:     ").append(getBucketsCopied())
                .append("\nobjects copied:     ").append(getObjectsCopied())
                .append("\nbytes copied:       ").append(getBytesCopied());
        if (!skipped.isEmpty()) {
            builder.append("\nskipped buckets:");
            skipped.forEach((bucket, reason) -> builder.append("\n  ").append(bucket).append(": ").append(reason));
        }
        if (!failures.isEmpty()) {
            builder.append("\nfailed buckets:");
            failures.forEach((bucket, reason) -> builder.append("\n  ").append(bucket).append(": ").append(reason));
        }
        return builder.toString();
    }
}
