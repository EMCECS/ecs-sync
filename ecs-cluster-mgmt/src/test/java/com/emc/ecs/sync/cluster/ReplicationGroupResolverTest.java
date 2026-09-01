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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ReplicationGroupResolverTest {
    private static final List<ReplicationGroup> TWO_GROUPS = Arrays.asList(
            new ReplicationGroup("urn:target:aaa", "rg1"),
            new ReplicationGroup("urn:target:bbb", "rg2"));
    private static final List<ReplicationGroup> ONE_GROUP =
            Collections.singletonList(new ReplicationGroup("urn:target:only", "the-only-one"));

    @Test
    public void testMatchesSourceGroupByName() {
        // the source ID is a different URN entirely; only the name correlates
        ReplicationGroupResolver resolver = new ReplicationGroupResolver(TWO_GROUPS, null);
        Assertions.assertEquals("urn:target:bbb", resolver.resolve("rg2"));
    }

    @Test
    public void testExplicitOverrideWins() {
        ReplicationGroupResolver resolver = new ReplicationGroupResolver(TWO_GROUPS, "rg1");
        Assertions.assertEquals("urn:target:aaa", resolver.resolve("rg2"));
        Assertions.assertEquals("urn:target:aaa", resolver.resolve(null));
    }

    @Test
    public void testOverrideMayBeAnIdInsteadOfAName() {
        // the UI selects a replication group from a list keyed on the ID
        ReplicationGroupResolver resolver = new ReplicationGroupResolver(TWO_GROUPS, "urn:target:bbb");
        Assertions.assertEquals("urn:target:bbb", resolver.resolve(null));
    }

    @Test
    public void testUnknownOverrideIsRejectedUpFront() {
        ConfigurationException e = Assertions.assertThrows(ConfigurationException.class,
                () -> new ReplicationGroupResolver(TWO_GROUPS, "nope"));
        Assertions.assertTrue(e.getMessage().contains("rg1"), "should list the available groups: " + e.getMessage());
    }

    @Test
    public void testSingleTargetGroupIsUsedAsFallback() {
        ReplicationGroupResolver resolver = new ReplicationGroupResolver(ONE_GROUP, null);
        Assertions.assertEquals("urn:target:only", resolver.resolve("some-unknown-source-group"));
        Assertions.assertEquals("urn:target:only", resolver.resolve(null));
    }

    @Test
    public void testAmbiguousMappingFailsRatherThanGuessing() {
        ReplicationGroupResolver resolver = new ReplicationGroupResolver(TWO_GROUPS, null);
        Assertions.assertThrows(ConfigurationException.class, () -> resolver.resolve("unknown-group"));
        Assertions.assertThrows(ConfigurationException.class, () -> resolver.resolve(null));
    }

    @Test
    public void testTargetWithNoReplicationGroupsIsRejected() {
        Assertions.assertThrows(ConfigurationException.class,
                () -> new ReplicationGroupResolver(Collections.emptyList(), null));
    }
}
