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
package com.emc.ecs.sync.storage;

import com.emc.atmos.AtmosException;
import com.emc.atmos.api.AtmosApi;
import com.emc.atmos.api.ObjectPath;
import com.emc.atmos.api.jersey.AtmosApiClient;
import com.emc.atmos.api.request.ListDirectoryRequest;
import com.emc.ecs.sync.AbstractEndToEndTest;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.storage.AtmosConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class AtmosEndToEndTest extends AbstractEndToEndTest {
    private static final Logger log = LoggerFactory.getLogger(AtmosEndToEndTest.class);

    @Test
    public void testAtmos() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        final String rootPath = "/ecs-sync-atmos-test/";
        String endpoints = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_ENDPOINTS);
        String uid = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_UID);
        String secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_ATMOS_SECRET);
        Assumptions.assumeTrue(endpoints != null && uid != null && secretKey != null);

        Protocol protocol = Protocol.http;
        List<String> hosts = new ArrayList<>();
        List<URI> endpointUris = new ArrayList<>();
        int port = -1;
        for (String endpoint : endpoints.split(",")) {
            URI uri = new URI(endpoint);
            endpointUris.add(uri);
            protocol = Protocol.valueOf(uri.getScheme().toLowerCase());
            port = uri.getPort();
            hosts.add(uri.getHost());
        }

        // make sure the Atmos namespace path does not already exist (this will mess up the test)
        AtmosApi atmos = new AtmosApiClient(new com.emc.atmos.api.AtmosConfig(uid, secretKey, endpointUris.toArray(new URI[0])));
        try {
            atmos.listDirectory(new ListDirectoryRequest().path(new ObjectPath(rootPath)));
            Assertions.fail("Atmos path " + rootPath + " already exists");
        } catch (AtmosException e) {
            if (e.getErrorCode() != 1003) throw e;
        }

        AtmosConfig atmosConfig = new AtmosConfig();
        atmosConfig.setProtocol(protocol);
        atmosConfig.setHosts(hosts.toArray(new String[0]));
        atmosConfig.setPort(port);
        atmosConfig.setUid(uid);
        atmosConfig.setSecret(secretKey);
        atmosConfig.setPath(rootPath);
        atmosConfig.setAccessType(AtmosConfig.AccessType.namespace);
        // make sure we don't hang forever on a stuck read
        System.setProperty("http.socket.timeout", "30000");

        String[] validGroups = new String[]{"other"};
        String[] validPermissions = new String[]{"READ", "WRITE", "FULL_CONTROL"};

        TestConfig testConfig = new TestConfig();
        testConfig.setObjectOwner(uid.substring(uid.lastIndexOf('/') + 1));
        testConfig.setValidGroups(validGroups);
        testConfig.setValidPermissions(validPermissions);

        ObjectAcl template = new ObjectAcl();
        template.addGroupGrant("other", "NONE");

        try {
            // TODO: ACL verification fails in ECS 3.6 because directory grants don't seem to apply anymore...
            //       setting syncAcl to false here in order to test everything else end-to-end...
            //       need to determine whether to file a bug against ECS
            multiEndToEndTest(atmosConfig, testConfig, template, false);
        } finally {
            try {
                // wait a maximum of 30 seconds for this delete call (it has been known to take a very long time)
                CompletableFuture.supplyAsync(() -> {
                    atmos.delete(new ObjectPath(rootPath));
                    return null;
                }).get(30, TimeUnit.SECONDS);
            } catch (Throwable t) {
                log.warn("could not delete bucket", t);
            }
        }
    }
}
