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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Decides the target namespace name for each source namespace.
 * <p>
 * A namespace name is used as-is when it is free on the target. When it is already taken, the configured suffix is
 * appended ({@code ns1} -> {@code ns1_target}); if that is taken too, a counter is added ({@code ns1_target2},
 * {@code ns1_target3}, ...) until a free name is found. This is what allows a source and target to be the same
 * cluster: every namespace is guaranteed to be renamed, so bucket names never collide.
 * <p>
 * When a namespace is renamed, object user IDs are renamed with the same suffix. This is required because ECS can be
 * configured with GLOBAL user scope, in which an object user ID must be unique across the entire cluster - so
 * {@code alice} in {@code ns1} cannot be recreated as {@code alice} in {@code ns1_target} on the same cluster.
 */
public class NamespaceMapper {
    private static final Logger log = LoggerFactory.getLogger(NamespaceMapper.class);

    private final String suffix;
    private final Map<String, String> explicitMap;
    private final Set<String> taken;
    private final Map<String, Mapping> resolved = new LinkedHashMap<>();

    /**
     * @param suffix               suffix appended on conflict, e.g. {@code _target}
     * @param explicitMap          source-namespace -> target-namespace overrides; bypasses conflict detection
     * @param existingTargetNamespaces namespace names that already exist on the target cluster
     */
    public NamespaceMapper(String suffix, Map<String, String> explicitMap, Set<String> existingTargetNamespaces) {
        if (suffix == null || suffix.trim().isEmpty())
            throw new ConfigurationException("the target namespace suffix cannot be empty");
        this.suffix = suffix.trim();
        this.explicitMap = explicitMap == null ? new LinkedHashMap<>() : explicitMap;
        this.taken = new HashSet<>(existingTargetNamespaces);
    }

    /**
     * Resolves (and remembers) the target namespace for a source namespace. Names handed out during this run are
     * reserved, so two source namespaces can never be mapped onto the same target namespace.
     */
    public Mapping map(String sourceNamespace) {
        Mapping existing = resolved.get(sourceNamespace);
        if (existing != null) return existing;

        Mapping mapping;
        String override = explicitMap.get(sourceNamespace);
        if (override != null) {
            // an explicit target name is used verbatim; users are still renamed if the namespace name changed, since
            // the reason for renaming (cluster-unique user IDs) applies regardless of how the name was chosen
            mapping = new Mapping(sourceNamespace, override, override.equals(sourceNamespace) ? "" : suffix);
            if (taken.contains(override)) {
                log.warn("namespace {} is explicitly mapped to {}, which already exists on the target - its existing "
                        + "contents will be added to, not replaced", sourceNamespace, override);
            }
        } else if (!taken.contains(sourceNamespace)) {
            mapping = new Mapping(sourceNamespace, sourceNamespace, "");
        } else {
            String candidateSuffix = suffix;
            int counter = 1;
            while (taken.contains(sourceNamespace + candidateSuffix)) {
                counter++;
                candidateSuffix = suffix + counter;
            }
            mapping = new Mapping(sourceNamespace, sourceNamespace + candidateSuffix, candidateSuffix);
            log.info("namespace {} already exists on the target; it will be created as {} (object users are renamed "
                    + "with the same suffix)", sourceNamespace, mapping.getTargetNamespace());
        }

        taken.add(mapping.getTargetNamespace());
        resolved.put(sourceNamespace, mapping);
        return mapping;
    }

    public Map<String, Mapping> getResolved() {
        return resolved;
    }

    /**
     * The source-to-target naming decision for one namespace.
     */
    public static class Mapping {
        private final String sourceNamespace;
        private final String targetNamespace;
        private final String appliedSuffix;

        public Mapping(String sourceNamespace, String targetNamespace, String appliedSuffix) {
            this.sourceNamespace = sourceNamespace;
            this.targetNamespace = targetNamespace;
            this.appliedSuffix = appliedSuffix;
        }

        public String getSourceNamespace() {
            return sourceNamespace;
        }

        public String getTargetNamespace() {
            return targetNamespace;
        }

        public String getAppliedSuffix() {
            return appliedSuffix;
        }

        public boolean isRenamed() {
            return !appliedSuffix.isEmpty();
        }

        /**
         * The target-side ID for an object user. Renamed with the same suffix as the namespace, so that clusters with
         * GLOBAL user scope do not reject it as a duplicate.
         */
        public String mapUserId(String sourceUserId) {
            if (sourceUserId == null) return null;
            return isRenamed() ? sourceUserId + appliedSuffix : sourceUserId;
        }

        @Override
        public String toString() {
            return isRenamed() ? sourceNamespace + " -> " + targetNamespace : sourceNamespace + " (unchanged)";
        }
    }
}
