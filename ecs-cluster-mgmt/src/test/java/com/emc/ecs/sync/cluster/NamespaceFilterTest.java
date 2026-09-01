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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NamespaceFilterTest {
    private static final List<EcsNamespace> NAMESPACES = Arrays.asList(
            new EcsNamespace("prod"), new EcsNamespace("staging"), new EcsNamespace("system"));

    private static List<String> names(List<EcsNamespace> namespaces) {
        return namespaces.stream().map(EcsNamespace::getName).collect(Collectors.toList());
    }

    @Test
    public void testNoRulesAcceptsEverything() {
        NamespaceFilter filter = new NamespaceFilter(null, null);
        Assertions.assertEquals(Arrays.asList("prod", "staging", "system"), names(filter.apply(NAMESPACES)));
    }

    @Test
    public void testIncludeList() {
        NamespaceFilter filter = new NamespaceFilter(new String[]{"prod", "staging"}, null);
        Assertions.assertEquals(Arrays.asList("prod", "staging"), names(filter.apply(NAMESPACES)));
    }

    @Test
    public void testExcludeList() {
        NamespaceFilter filter = new NamespaceFilter(null, new String[]{"system"});
        Assertions.assertEquals(Arrays.asList("prod", "staging"), names(filter.apply(NAMESPACES)));
    }

    @Test
    public void testExcludeWinsOverInclude() {
        NamespaceFilter filter = new NamespaceFilter(new String[]{"prod", "system"}, new String[]{"system"});
        Assertions.assertEquals(Arrays.asList("prod"), names(filter.apply(NAMESPACES)));
    }

    @Test
    public void testWhitespaceAndEmptyEntriesAreIgnored() {
        NamespaceFilter filter = new NamespaceFilter(new String[]{" prod ", "", null}, null);
        Assertions.assertEquals(Arrays.asList("prod"), names(filter.apply(NAMESPACES)));
    }

    @Test
    public void testUnmatchedIncludesAreReported() {
        NamespaceFilter filter = new NamespaceFilter(new String[]{"prod", "nonexistent"}, null);
        Assertions.assertEquals(java.util.Collections.singleton("nonexistent"),
                filter.unmatchedIncludes(NAMESPACES));
    }
}
