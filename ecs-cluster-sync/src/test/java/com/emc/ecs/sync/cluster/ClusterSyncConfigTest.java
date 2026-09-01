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
import com.emc.ecs.sync.config.Protocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import javax.xml.bind.JAXBContext;
import java.io.File;
import java.io.StringReader;
import java.util.Map;

public class ClusterSyncConfigTest {
    private static ClusterEndpoint endpoint(String host) {
        ClusterEndpoint endpoint = new ClusterEndpoint();
        endpoint.setManagementEndpoint(host);
        endpoint.setAdminUsername("root");
        endpoint.setAdminPassword("secret");
        return endpoint;
    }

    private static ClusterSyncConfig validConfig() {
        ClusterSyncConfig config = new ClusterSyncConfig();
        config.setSource(endpoint("10.0.0.1"));
        config.setTarget(endpoint("10.1.0.1"));
        return config;
    }

    @Test
    public void testValidConfig() {
        validConfig().validate();
    }

    @Test
    public void testMissingClusterIsRejected() {
        ClusterSyncConfig config = new ClusterSyncConfig();
        config.setSource(endpoint("10.0.0.1"));
        Assertions.assertThrows(ConfigurationException.class, config::validate);
    }

    @Test
    public void testMissingCredentialsAreRejected() {
        ClusterSyncConfig config = validConfig();
        config.getTarget().setAdminPassword(null);
        ConfigurationException e = Assertions.assertThrows(ConfigurationException.class, config::validate);
        Assertions.assertTrue(e.getMessage().contains("target"), e.getMessage());
    }

    @Test
    public void testSameClusterIsAllowedWithATargetReplicationGroup() {
        // storage pool migration within one cluster
        ClusterSyncConfig config = validConfig();
        config.getTarget().setManagementEndpoint("10.0.0.1");
        config.getOptions().setTargetReplicationGroup("rg-on-new-pool");

        config.validate();
        Assertions.assertTrue(config.isSameCluster());
    }

    @Test
    public void testSameClusterWithoutReplicationGroupIsRejected() {
        // would copy into the same storage pool, which is pointless
        ClusterSyncConfig config = validConfig();
        config.getTarget().setManagementEndpoint("10.0.0.1");

        ConfigurationException e = Assertions.assertThrows(ConfigurationException.class, config::validate);
        Assertions.assertTrue(e.getMessage().contains("replication group"), e.getMessage());
    }

    @Test
    public void testSameClusterCannotSkipNamespaceCreation() {
        ClusterSyncConfig config = validConfig();
        config.getTarget().setManagementEndpoint("10.0.0.1");
        config.getOptions().setTargetReplicationGroup("rg2");
        config.getOptions().setSkipNamespaceCreation(true);

        Assertions.assertThrows(ConfigurationException.class, config::validate);
    }

    @Test
    public void testTwoClustersDoNotRequireAReplicationGroup() {
        ClusterSyncConfig config = validConfig();
        config.validate();
        Assertions.assertFalse(config.isSameCluster());
    }

    @Test
    public void testNamespaceMapParsing() {
        ClusterSyncOptions options = new ClusterSyncOptions();
        options.setNamespaceMap(new String[]{"ns1=ns1_target", " ns2 = archive ", "", null});

        Map<String, String> parsed = options.parseNamespaceMap();
        Assertions.assertEquals(2, parsed.size());
        Assertions.assertEquals("ns1_target", parsed.get("ns1"));
        Assertions.assertEquals("archive", parsed.get("ns2"));
    }

    @Test
    public void testMalformedNamespaceMapIsRejected() {
        for (String bad : new String[]{"no-separator", "=missing-source", "missing-target="}) {
            ClusterSyncOptions options = new ClusterSyncOptions();
            options.setNamespaceMap(new String[]{bad});
            Assertions.assertThrows(ConfigurationException.class, options::parseNamespaceMap,
                    "should have rejected: " + bad);
        }
    }

    @Test
    public void testMalformedNamespaceMapFailsValidation() {
        ClusterSyncConfig config = validConfig();
        config.getOptions().setNamespaceMap(new String[]{"garbage"});
        Assertions.assertThrows(ConfigurationException.class, config::validate);
    }

    @Test
    public void testDefaultNamespaceSuffix() {
        Assertions.assertEquals("_target", new ClusterSyncOptions().getTargetNamespaceSuffix());
    }

    @Test
    public void testDataEndpointsDefaultToManagementEndpoint() {
        // the whole point of the feature: a cluster is specified by one IP plus credentials
        Assertions.assertArrayEquals(new String[]{"10.0.0.1"}, endpoint("10.0.0.1").effectiveDataEndpoints());
    }

    @Test
    public void testExplicitDataEndpointsWin() {
        ClusterEndpoint endpoint = endpoint("10.0.0.1");
        endpoint.setDataEndpoints(new String[]{"10.0.0.2", "10.0.0.3"});
        Assertions.assertArrayEquals(new String[]{"10.0.0.2", "10.0.0.3"}, endpoint.effectiveDataEndpoints());
    }

    @Test
    public void testManagementBaseUrl() {
        Assertions.assertEquals("https://10.0.0.1:4443", endpoint("10.0.0.1").managementBaseUrl());
    }

    @Test
    public void testShippedSampleIsValid() throws Exception {
        File sample = new File("../sample/ecs-cluster-replication.xml");
        Assumptions.assumeTrue(sample.exists(), "sample not found at " + sample.getAbsolutePath());

        ClusterSyncConfig config = EcsClusterSyncCli.loadXml(sample);
        config.validate();

        Assertions.assertNotNull(config.getSource().getManagementEndpoint());
        Assertions.assertNotNull(config.getTarget().getManagementEndpoint());
        Assertions.assertArrayEquals(new String[]{"system"}, config.getOptions().getExcludeNamespaces());
    }

    @Test
    public void testXmlRoundTrip() throws Exception {
        String xml = "<clusterSyncConfig>"
                + "<source>"
                + "<managementEndpoint>10.0.0.1</managementEndpoint>"
                + "<adminUsername>root</adminUsername>"
                + "<adminPassword>pw1</adminPassword>"
                + "<trustSelfSignedCertificates>true</trustSelfSignedCertificates>"
                + "</source>"
                + "<target>"
                + "<managementEndpoint>10.1.0.1</managementEndpoint>"
                + "<adminUsername>admin</adminUsername>"
                + "<adminPassword>pw2</adminPassword>"
                + "<dataPort>9021</dataPort>"
                + "<dataProtocol>https</dataProtocol>"
                + "</target>"
                + "<options>"
                + "<excludeNamespaces>system</excludeNamespaces>"
                + "<threadCount>32</threadCount>"
                + "<verify>true</verify>"
                + "</options>"
                + "</clusterSyncConfig>";

        ClusterSyncConfig config = (ClusterSyncConfig) JAXBContext.newInstance(ClusterSyncConfig.class)
                .createUnmarshaller().unmarshal(new StringReader(xml));
        config.validate();

        Assertions.assertEquals("10.0.0.1", config.getSource().getManagementEndpoint());
        Assertions.assertEquals("root", config.getSource().getAdminUsername());
        Assertions.assertTrue(config.getSource().isTrustSelfSignedCertificates());
        // defaults still apply to values the document does not set
        Assertions.assertEquals(Protocol.https, config.getSource().getManagementProtocol());
        Assertions.assertEquals(ClusterEndpoint.DEFAULT_MANAGEMENT_PORT, config.getSource().getManagementPort());
        Assertions.assertEquals(Protocol.http, config.getSource().getDataProtocol());

        Assertions.assertEquals(9021, config.getTarget().getDataPort());
        Assertions.assertEquals(Protocol.https, config.getTarget().getDataProtocol());

        Assertions.assertArrayEquals(new String[]{"system"}, config.getOptions().getExcludeNamespaces());
        Assertions.assertEquals(32, config.getOptions().getThreadCount());
        Assertions.assertTrue(config.getOptions().isVerify());
        // versions are copied by default
        Assertions.assertTrue(config.getOptions().isIncludeVersions());
    }
}
