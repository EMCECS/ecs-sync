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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class NamespaceMapperTest {
    private static final String SUFFIX = "_target";

    private static NamespaceMapper mapper(String... existingOnTarget) {
        return new NamespaceMapper(SUFFIX, null, new HashSet<>(java.util.Arrays.asList(existingOnTarget)));
    }

    @Test
    public void testFreeNameIsUsedAsIs() {
        NamespaceMapper.Mapping mapping = mapper().map("ns1");

        Assertions.assertEquals("ns1", mapping.getTargetNamespace());
        Assertions.assertFalse(mapping.isRenamed());
        // users keep their original IDs when nothing was renamed
        Assertions.assertEquals("alice", mapping.mapUserId("alice"));
    }

    @Test
    public void testConflictGetsSuffix() {
        NamespaceMapper.Mapping mapping = mapper("ns1").map("ns1");

        Assertions.assertEquals("ns1_target", mapping.getTargetNamespace());
        Assertions.assertTrue(mapping.isRenamed());
        Assertions.assertEquals("_target", mapping.getAppliedSuffix());
        // user IDs must be renamed too, since ECS GLOBAL user scope requires cluster-unique IDs
        Assertions.assertEquals("alice_target", mapping.mapUserId("alice"));
    }

    @Test
    public void testSuffixedNameAlsoTakenAddsCounter() {
        Assertions.assertEquals("ns1_target2",
                mapper("ns1", "ns1_target").map("ns1").getTargetNamespace());
        Assertions.assertEquals("ns1_target3",
                mapper("ns1", "ns1_target", "ns1_target2").map("ns1").getTargetNamespace());
    }

    @Test
    public void testCounterIsReflectedInUserSuffix() {
        NamespaceMapper.Mapping mapping = mapper("ns1", "ns1_target").map("ns1");

        Assertions.assertEquals("ns1_target2", mapping.getTargetNamespace());
        Assertions.assertEquals("_target2", mapping.getAppliedSuffix());
        Assertions.assertEquals("alice_target2", mapping.mapUserId("alice"));
    }

    @Test
    public void testSameCallIsStable() {
        NamespaceMapper mapper = mapper("ns1");

        // resolving twice must not allocate a second name
        Assertions.assertEquals("ns1_target", mapper.map("ns1").getTargetNamespace());
        Assertions.assertEquals("ns1_target", mapper.map("ns1").getTargetNamespace());
        Assertions.assertEquals(1, mapper.getResolved().size());
    }

    @Test
    public void testNamesAllocatedInThisRunAreReserved() {
        // ns1 is taken, so ns1 -> ns1_target. A source namespace literally called "ns1_target" must not collide
        // with the name we just handed out.
        NamespaceMapper mapper = mapper("ns1");

        Assertions.assertEquals("ns1_target", mapper.map("ns1").getTargetNamespace());
        Assertions.assertEquals("ns1_target_target", mapper.map("ns1_target").getTargetNamespace());
    }

    @Test
    public void testSameClusterRenamesEveryNamespace() {
        // a same-cluster migration sees every source namespace as already existing
        NamespaceMapper mapper = mapper("prod", "staging");

        Assertions.assertEquals("prod_target", mapper.map("prod").getTargetNamespace());
        Assertions.assertEquals("staging_target", mapper.map("staging").getTargetNamespace());
    }

    @Test
    public void testExplicitMappingWins() {
        Map<String, String> explicit = new LinkedHashMap<>();
        explicit.put("ns1", "archive2024");
        NamespaceMapper mapper = new NamespaceMapper(SUFFIX, explicit, new HashSet<>(Collections.singletonList("ns1")));

        NamespaceMapper.Mapping mapping = mapper.map("ns1");
        Assertions.assertEquals("archive2024", mapping.getTargetNamespace());
        // the namespace name changed, so users are still suffixed
        Assertions.assertTrue(mapping.isRenamed());
        Assertions.assertEquals("alice_target", mapping.mapUserId("alice"));
    }

    @Test
    public void testExplicitMappingToSameNameKeepsUserIds() {
        Map<String, String> explicit = new LinkedHashMap<>();
        explicit.put("ns1", "ns1");
        NamespaceMapper mapper = new NamespaceMapper(SUFFIX, explicit, new HashSet<>(Collections.singletonList("ns1")));

        NamespaceMapper.Mapping mapping = mapper.map("ns1");
        Assertions.assertEquals("ns1", mapping.getTargetNamespace());
        Assertions.assertFalse(mapping.isRenamed());
        Assertions.assertEquals("alice", mapping.mapUserId("alice"));
    }

    @Test
    public void testExplicitMappingEnablesDeterministicResume() {
        // second run: ns1 and ns1_target both exist because the first run created ns1_target. Without an explicit
        // map the counter would allocate ns1_target2; with it, the run resumes into ns1_target.
        Set<String> existing = new HashSet<>(java.util.Arrays.asList("ns1", "ns1_target"));
        Assertions.assertEquals("ns1_target2",
                new NamespaceMapper(SUFFIX, null, existing).map("ns1").getTargetNamespace());

        Map<String, String> explicit = new LinkedHashMap<>();
        explicit.put("ns1", "ns1_target");
        Assertions.assertEquals("ns1_target",
                new NamespaceMapper(SUFFIX, explicit, existing).map("ns1").getTargetNamespace());
    }

    @Test
    public void testCustomSuffix() {
        NamespaceMapper mapper = new NamespaceMapper("_pool2", null,
                new HashSet<>(Collections.singletonList("ns1")));
        NamespaceMapper.Mapping mapping = mapper.map("ns1");

        Assertions.assertEquals("ns1_pool2", mapping.getTargetNamespace());
        Assertions.assertEquals("alice_pool2", mapping.mapUserId("alice"));
    }

    @Test
    public void testEmptySuffixIsRejected() {
        Assertions.assertThrows(ConfigurationException.class,
                () -> new NamespaceMapper("", null, Collections.emptySet()));
        Assertions.assertThrows(ConfigurationException.class,
                () -> new NamespaceMapper(null, null, Collections.emptySet()));
    }

    @Test
    public void testNullUserIdIsTolerated() {
        Assertions.assertNull(mapper("ns1").map("ns1").mapUserId(null));
    }
}
