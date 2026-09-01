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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applies the include/exclude namespace rules.
 */
public final class NamespaceFilter {
    private final Set<String> includes;
    private final Set<String> excludes;

    public NamespaceFilter(String[] includes, String[] excludes) {
        this.includes = toSet(includes);
        this.excludes = toSet(excludes);
    }

    private static Set<String> toSet(String[] values) {
        if (values == null) return Collections.emptySet();
        return Arrays.stream(values)
                .filter(v -> v != null && !v.trim().isEmpty())
                .map(String::trim)
                .collect(Collectors.toSet());
    }

    public boolean accepts(String namespace) {
        if (!includes.isEmpty() && !includes.contains(namespace)) return false;
        return !excludes.contains(namespace);
    }

    public List<EcsNamespace> apply(List<EcsNamespace> namespaces) {
        return namespaces.stream()
                .filter(ns -> accepts(ns.getName()))
                .collect(Collectors.toList());
    }

    /**
     * @return names in the include list that were not present on the source cluster
     */
    public Set<String> unmatchedIncludes(List<EcsNamespace> namespaces) {
        Set<String> missing = new HashSet<>(includes);
        namespaces.forEach(ns -> missing.remove(ns.getName()));
        return missing;
    }
}
