/*
 * Copyright (c) 2019-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.MetadataConfig;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MetadataFilterTest {
    private TestStorage storage = new TestStorage();

    @Test
    public void testChangeKeys() {
        String oldKey = "foo", newKey = "bar";
        String key1 = "key1", key2 = "key2";

        String k1 = "foo", v1 = "blah";
        String k2 = "Foo", v2 = "blue";
        String k3 = "xx", v3 = "blee";

        MetadataConfig config = new MetadataConfig();
        config.setChangeMetadataKeys(new String[]{String.format("%s=%s", oldKey, newKey)});

        Target target = new Target();
        MetadataFilter filter = buildFilter(config, target);

        filter.filter(buildContext(key1, map().md(k2, v2).md(k3, v3), filter.getOptions()));
        filter.filter(buildContext(key2, map().md(k1, v1).md(k2, v2), filter.getOptions()));

        Assertions.assertEquals(v2, target.objectMap.get(key1).getMetadata().getUserMetadataValue(k2));
        Assertions.assertEquals(v3, target.objectMap.get(key1).getMetadata().getUserMetadataValue(k3));
        Assertions.assertEquals(2, target.objectMap.get(key1).getMetadata().getUserMetadata().size());
        Assertions.assertNull(target.objectMap.get(key1).getMetadata().getUserMetadataValue(k1));

        Assertions.assertEquals(v1, target.objectMap.get(key2).getMetadata().getUserMetadataValue(newKey));
        Assertions.assertEquals(v2, target.objectMap.get(key2).getMetadata().getUserMetadataValue(k2));
        Assertions.assertEquals(2, target.objectMap.get(key2).getMetadata().getUserMetadata().size());
        Assertions.assertNull(target.objectMap.get(key2).getMetadata().getUserMetadataValue(k1));
    }

    private MDMap map() {
        return new MDMap();
    }

    private MetadataFilter buildFilter(MetadataConfig config, Target target) {
        SyncOptions options = new SyncOptions();

        MetadataFilter filter = new MetadataFilter();
        filter.withConfig(config).withOptions(options);
        filter.configure(storage, Collections.singletonList(filter).iterator(), storage);

        filter.setNext(target);

        return filter;
    }

    private ObjectContext buildContext(String path, Map<String, String> userMeta, SyncOptions options) {
        ObjectSummary summary = new ObjectSummary("/context/" + path, false, 0);
        ObjectContext context = new ObjectContext().withOptions(options).withSourceSummary(summary);
        ObjectMetadata meta = new ObjectMetadata().withContentLength(0).withDirectory(false);
        meta.getUserMetadataValueMap().putAll(userMeta);
        context.setObject(new SyncObject(storage, path, meta, new ByteArrayInputStream(new byte[0]), new ObjectAcl()));
        return context;
    }

    class MDMap extends HashMap<String, String> {
        MDMap md(String key, String value) {
            put(key, value);
            return this;
        }
    }

    class Target extends AbstractFilter {
        Map<String, SyncObject> objectMap = new TreeMap<>();

        @Override
        public void filter(ObjectContext objectContext) {
            objectMap.put(objectContext.getObject().getRelativePath(), objectContext.getObject());
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            throw new UnsupportedOperationException("this filter does not support verification");
        }
    }
}
