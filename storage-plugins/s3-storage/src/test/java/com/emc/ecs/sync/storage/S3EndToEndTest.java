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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.emc.ecs.sync.AbstractEndToEndTest;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;

public class S3EndToEndTest extends AbstractEndToEndTest {
    Logger log = LoggerFactory.getLogger(S3EndToEndTest.class);

    @Test
    public void testEcsS3() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        final String bucket = "ecs-sync-ecs-s3-test-bucket";
        final String endpoint = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_SECRET_KEY);
        final boolean useVHost = Boolean.parseBoolean(syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_VHOST));
        Assumptions.assumeTrue(endpoint != null && accessKey != null && secretKey != null);
        URI endpointUri = new URI(endpoint);

        S3Config s3Config;
        if (useVHost) {
            s3Config = new S3Config(endpointUri);
        } else {
            s3Config = new S3Config(com.emc.object.Protocol.valueOf(endpointUri.getScheme().toUpperCase()), endpointUri.getHost());
        }
        s3Config.withPort(endpointUri.getPort()).withUseVHost(useVHost).withIdentity(accessKey).withSecretKey(secretKey);

        s3Config.setConnectTimeout(5000); // fail fast if config is bad

        S3Client s3 = new S3JerseyClient(s3Config);
        s3.createBucket(bucket);
        String ownerId = s3.getBucketAcl(bucket).getOwner().getId();

        // for testing ACLs
        String authUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
        String everyone = "http://acs.amazonaws.com/groups/global/AllUsers";
        String[] validGroups = {authUsers, everyone};
        String[] validPermissions = {"READ", "WRITE", "FULL_CONTROL"};

        EcsS3Config ecsS3Config = new EcsS3Config();
        if (endpointUri.getScheme() != null)
            ecsS3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        ecsS3Config.setHost(endpointUri.getHost());
        ecsS3Config.setPort(endpointUri.getPort());
        ecsS3Config.setAccessKey(accessKey);
        ecsS3Config.setSecretKey(secretKey);
        ecsS3Config.setEnableVHosts(useVHost);
        ecsS3Config.setBucketName(bucket);
        ecsS3Config.setPreserveDirectories(true);
        // make sure we don't hang forever on a stuck read
        ecsS3Config.setSocketReadTimeoutMs(30_000);

        TestConfig testConfig = new TestConfig();
        testConfig.setObjectOwner(ownerId);
        testConfig.setValidGroups(validGroups);
        testConfig.setValidPermissions(validPermissions);

        try {
            multiEndToEndTest(ecsS3Config, testConfig, true);
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                log.warn("could not delete bucket", t);
            }
        }
    }

    @Test
    public void testS3() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        final String bucket = "ecs-sync-s3-test-bucket";
        final String endpoint = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ENDPOINT);
        final String accessKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_ACCESS_KEY_ID);
        final String secretKey = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_SECRET_KEY);
        final String region = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_S3_REGION);
        Assumptions.assumeTrue(endpoint != null && accessKey != null && secretKey != null);
        URI endpointUri = new URI(endpoint);

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");
        config.withConnectionTimeout(5000); // fail fast if config is bad
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        builder.withClientConfiguration(config);
        builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region));

        AmazonS3 s3 = builder.build();
        s3.createBucket(bucket);
        String ownerId = s3.getBucketAcl(bucket).getOwner().getId();

        // for testing ACLs
        String authUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
        String everyone = "http://acs.amazonaws.com/groups/global/AllUsers";
        String[] validGroups = {authUsers, everyone};
        String[] validPermissions = {"READ", "WRITE", "FULL_CONTROL"};

        AwsS3Config awsS3Config = new AwsS3Config();
        if (endpointUri.getScheme() != null)
            awsS3Config.setProtocol(Protocol.valueOf(endpointUri.getScheme().toLowerCase()));
        awsS3Config.setHost(endpointUri.getHost());
        awsS3Config.setPort(endpointUri.getPort());
        awsS3Config.setAccessKey(accessKey);
        awsS3Config.setSecretKey(secretKey);
        awsS3Config.setRegion(region);
        awsS3Config.setLegacySignatures(true);
        awsS3Config.setDisableVHosts(true);
        awsS3Config.setBucketName(bucket);
        awsS3Config.setPreserveDirectories(true);

        TestConfig testConfig = new TestConfig();
        testConfig.setObjectOwner(ownerId);
        testConfig.setValidGroups(validGroups);
        testConfig.setValidPermissions(validPermissions);

        try {
            multiEndToEndTest(awsS3Config, testConfig, true);
        } finally {
            try {
                s3.deleteBucket(bucket);
            } catch (Throwable t) {
                log.warn("could not delete bucket", t);
            }
        }
    }
}
