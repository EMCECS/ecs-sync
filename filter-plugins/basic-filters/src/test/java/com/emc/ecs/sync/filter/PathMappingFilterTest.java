/*
 * Copyright (c) 2021-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.PathMappingConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

import static com.emc.ecs.sync.config.filter.PathMappingConfig.MapSource.Metadata;
import static com.emc.ecs.sync.config.filter.PathMappingConfig.MapSource.RegEx;

public class PathMappingFilterTest {
    TestStorage storage = new TestStorage();
    TestConfig testConfig = new TestConfig().withObjectCount(0).withDiscardData(false);
    Map<String, String> pathMap;
    List<String> directories = Collections.singletonList("foo");
    String testStoragePrefix = TestStorage.ROOT_PATH + "/";
    String mappingMetadataName = "mapped-path";
    String regexPattern = "^foo(/|$)";
    String regexReplacement = "bar$1";

    @BeforeEach
    public void setup() {
        pathMap = new HashMap<>();
        pathMap.put("foo", "bar");
        pathMap.put("foo/alpha", "bar/alpha");
        pathMap.put("foo/bravo", "bar/bravo");
        pathMap.put("foo/charlie", "bar/charlie");
        pathMap.put("foo/delta", "bar/delta");
        pathMap.put("foo/echo", "bar/echo");
        pathMap.put("foo/foxtrot", "bar/foxtrot");
    }

    @Test
    public void testCsvMapping() throws Exception {
        StringBuilder csvData = new StringBuilder();
        for (Map.Entry<String, String> entry : pathMap.entrySet()) {
            // source-list-file (first column) contains source identifiers (absolute paths), so here we have to add the
            // internal path prefix that it uses
            csvData.append(testStoragePrefix).append(entry.getKey()).append(",").append(entry.getValue()).append("\n");
        }

        // write the mapping file
        File csvMappingFile = File.createTempFile("id-mapping", "csv");
        csvMappingFile.deleteOnExit();
        Files.write(csvMappingFile.toPath(), csvData.toString().getBytes(StandardCharsets.UTF_8));

        // create mapping filter config
        // Assumes: default is CSV and store previous path in meta is enabled
        PathMappingConfig filterConfig = new PathMappingConfig();

        // need to turn off recursion when using a source-list-file
        SyncOptions options = new SyncOptions().withSourceListFile(csvMappingFile.toString()).withRecursive(false);

        TestStorage source = initializeSourceStorage(options, false);

        // build sync config
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setOptions(options);
        syncConfig.setFilters(Collections.singletonList(filterConfig));
        syncConfig.setTarget(testConfig);
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(source);
        sync.run();

        validateMappingResults((TestStorage) sync.getTarget());
    }

    @Test
    public void testMetadataMapping() {
        // create mapping filter config
        // Assumes: store previous path in meta is enabled
        PathMappingConfig filterConfig = new PathMappingConfig();
        filterConfig.setMapSource(Metadata);
        filterConfig.setMetadataName(mappingMetadataName);

        SyncOptions options = new SyncOptions();

        TestStorage source = initializeSourceStorage(options, true);

        // build sync config
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setOptions(options);
        syncConfig.setFilters(Collections.singletonList(filterConfig));
        syncConfig.setTarget(testConfig);
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(source);
        sync.run();

        validateMappingResults((TestStorage) sync.getTarget());
    }

    @Test
    public void testRegExMapping() {
        // create mapping filter config
        // Assumes: store previous path in meta is enabled
        PathMappingConfig filterConfig = new PathMappingConfig();
        filterConfig.setMapSource(RegEx);
        filterConfig.setRegExPattern(regexPattern);
        filterConfig.setRegExReplacementString(regexReplacement);

        SyncOptions options = new SyncOptions();

        TestStorage source = initializeSourceStorage(options, false);

        // build sync config
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setOptions(options);
        syncConfig.setFilters(Collections.singletonList(filterConfig));
        syncConfig.setTarget(testConfig);
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);
        sync.setSource(source);
        sync.run();

        validateMappingResults((TestStorage) sync.getTarget());
    }

    TestStorage initializeSourceStorage(SyncOptions options, boolean addMappingMetadata) {
        // "ingest" source keys in dummy storage
        TestStorage storage = new TestStorage();
        storage.withConfig(testConfig).withOptions(options);
        for (String key : pathMap.keySet()) {
            boolean directory = directories.contains(key);
            ObjectMetadata meta = new ObjectMetadata().withContentLength(0).withDirectory(directory);
            // add mapping metadata if necessary
            if (addMappingMetadata) meta.setUserMetadataValue(mappingMetadataName, pathMap.get(key));
            SyncObject syncObject = new SyncObject(this.storage, key, meta, new ByteArrayInputStream(new byte[0]), new ObjectAcl());
            // updateObject expects a storage identifier (absolute path), so here we have to add the
            // internal path prefix that it uses
            storage.updateObject(testStoragePrefix + key, syncObject);
        }
        return storage;
    }

    void validateMappingResults(TestStorage targetStorage) {
        // total object count matches
        Set<SyncObject> targetObjects = getAllObjects(targetStorage, null);
        Assertions.assertEquals(pathMap.size(), targetObjects.size());
        // target storage contains only target paths
        Set<String> targetPaths = targetObjects.stream().map(SyncObject::getRelativePath).collect(Collectors.toSet());
        Assertions.assertEquals(new HashSet<>(pathMap.values()), targetPaths);
        // check mapping and original path in metadata
        for (SyncObject object : targetObjects) {
            String originalPath = object.getMetadata().getUserMetadataValue(PathMappingConfig.META_PREVIOUS_NAME);
            Assertions.assertNotNull("original path not set in metadata", originalPath);
            Assertions.assertEquals(pathMap.get(originalPath), object.getRelativePath());
        }
    }

    Set<SyncObject> getAllObjects(TestStorage storage, ObjectSummary parent) {
        Set<SyncObject> objects = new HashSet<>();
        for (ObjectSummary summary : (parent != null ? storage.children(parent) : storage.allObjects())) {
            objects.add(storage.loadObject(summary.getIdentifier()));
            if (summary.isDirectory()) objects.addAll(getAllObjects(storage, summary));
        }
        return objects;
    }
}
