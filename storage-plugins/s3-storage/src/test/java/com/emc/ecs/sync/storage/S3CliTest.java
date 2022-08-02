/*
 * Copyright (c) 2014-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.AbstractCliTest;
import com.emc.ecs.sync.CliConfig;
import com.emc.ecs.sync.CliHelper;
import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class S3CliTest extends AbstractCliTest {
    @Test
    public void testEcsS3Cli() {
        String sourceBucket = "source-bucket";
        String targetBucket = "target-bucket";
        String sourceKeyPrefix = "source/prefix/";
        String targetKeyPrefix = "target/prefix/";
        String[] targetVdcs = {"(10.10.10.11,10.10.10.12)", "vdc2(1.2.3.4,1.2.3.5)", "10.10.20.11"};

        String[] args = new String[]{
                "-source", "ecs-s3:http://wuser1@SANITY.LOCAL:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@s3.company.com/" + sourceBucket + "/" + sourceKeyPrefix,
                "-target", "ecs-s3:https://root:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@" + ConfigUtil.join(targetVdcs) + ":9123/" + targetBucket + "/" + targetKeyPrefix,
                "--source-url-encode-keys",
                "--source-enable-v-host",
                "--source-no-smart-client",
                "--source-no-apache-client",
                "--target-enable-v-host",
                "--target-no-smart-client",
                "--source-include-versions",
                "--target-include-versions",
                "--target-no-preserve-directories",
                "--target-mpu-enabled",
                "--target-socket-read-timeout-ms", "60000"
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assertions.assertNotNull(source, "source is null");
        Assertions.assertTrue(source instanceof EcsS3Config, "source is not EcsS3Config");
        EcsS3Config s3Source = (EcsS3Config) source;

        Object target = syncConfig.getTarget();
        Assertions.assertNotNull(target, "target is null");
        Assertions.assertTrue(target instanceof EcsS3Config, "target is not EcsS3Config");
        EcsS3Config s3Target = (EcsS3Config) target;

        Assertions.assertEquals(Protocol.http, s3Source.getProtocol(), "source protocol mismatch");
        Assertions.assertEquals("s3.company.com", s3Source.getHost(), "source host mismatch");
        Assertions.assertNull(s3Source.getVdcs(), "source vdcs should be null");
        Assertions.assertEquals(-1, s3Source.getPort(), "source port mismatch");
        Assertions.assertEquals(sourceBucket, s3Source.getBucketName(), "source bucket mismatch");
        Assertions.assertEquals(sourceKeyPrefix, s3Source.getKeyPrefix());
        Assertions.assertTrue(s3Source.isUrlEncodeKeys(), "source url-encode-keys should be enabled");
        Assertions.assertTrue(s3Source.isEnableVHosts(), "source vhost should be enabled");
        Assertions.assertFalse(s3Source.isSmartClientEnabled(), "source smart-client should be disabled");
        Assertions.assertFalse(s3Source.isApacheClientEnabled(), "source apache-client should be disabled");
        Assertions.assertEquals(Protocol.https, s3Target.getProtocol(), "target protocol mismatch");
        Assertions.assertArrayEquals(targetVdcs, s3Target.getVdcs());
        Assertions.assertNull(s3Target.getHost(), "target host should be null");
        Assertions.assertEquals(9123, s3Target.getPort(), "target port mismatch");
        Assertions.assertEquals(targetBucket, s3Target.getBucketName(), "target bucket mismatch");
        Assertions.assertEquals(targetKeyPrefix, s3Target.getKeyPrefix());
        Assertions.assertTrue(s3Target.isEnableVHosts(), "target vhost should be enabled");
        Assertions.assertFalse(s3Target.isSmartClientEnabled(), "target smart-client should be disabled");
        Assertions.assertTrue(s3Target.isApacheClientEnabled(), "target apache-client should be enabled");
        Assertions.assertTrue(s3Target.isIncludeVersions(), "target versions should be enabled");
        Assertions.assertFalse(s3Target.isPreserveDirectories(), "target preserveDirectories should be disabled");
        Assertions.assertTrue(s3Target.isMpuEnabled(), "target MPU should be enabled");
        Assertions.assertEquals(60000, s3Target.getSocketReadTimeoutMs(), "target socket-read-timeout-ms mismatch");
    }

    @Test
    public void testS3Cli() {
        String sourceAccessKey = "amz_user1234567890";
        String sourceSecret = "HkayrXoENUQ3+VCMCa/aViS0tbpDs=";
        String sourceBucket = "source-bucket";
        String sourceUri = String.format("s3:%s:%s@/%s", sourceAccessKey, sourceSecret, sourceBucket);

        Protocol targetProtocol = Protocol.http;
        String targetHost = "s3.company.com";
        int targetPort = 9020;
        String targetAccessKey = "wuser1@SANITY.LOCAL";
        String targetSecret = "awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi";
        String targetBucket = "target-bucket";
        String targetKeyPrefix = "target/prefix/";
        String targetUri = String.format("s3:%s://%s:%s@%s:%d/%s/%s",
                targetProtocol, targetAccessKey, targetSecret, targetHost, targetPort, targetBucket, targetKeyPrefix);
        int mpuThreshold = 100, mpuPartSize = 25, socketTimeout = 5000;

        String[] args = new String[]{
                "-source", sourceUri,
                "-target", targetUri,
                "--source-include-versions",
                "--target-create-bucket",
                "--target-disable-v-hosts",
                "--target-include-versions",
                "--target-legacy-signatures",
                "--target-no-preserve-directories",
                "--target-mpu-threshold-mb", "" + mpuThreshold,
                "--target-mpu-part-size-mb", "" + mpuPartSize,
                "--target-socket-timeout-ms", "" + socketTimeout
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assertions.assertNotNull(source, "source is null");
        Assertions.assertTrue(source instanceof AwsS3Config, "source is not AwsS3Config");
        AwsS3Config s3Source = (AwsS3Config) source;

        Object target = syncConfig.getTarget();
        Assertions.assertNotNull(target, "target is null");
        Assertions.assertTrue(target instanceof AwsS3Config, "target is not AwsS3Config");
        AwsS3Config s3Target = (AwsS3Config) target;

        Assertions.assertEquals(sourceUri, s3Source.getUri(false), "source URI mismatch");
        Assertions.assertNull(s3Source.getProtocol(), "source protocol should be null");
        Assertions.assertEquals(sourceAccessKey, s3Source.getAccessKey(), "source access key mismatch");
        Assertions.assertEquals(sourceSecret, s3Source.getSecretKey(), "source secret key mismatch");
        Assertions.assertNull(s3Source.getHost(), "source host should be null");
        Assertions.assertEquals(-1, s3Source.getPort(), "source port mismatch");
        Assertions.assertEquals(sourceBucket, s3Source.getBucketName(), "source bucket mismatch");
        Assertions.assertNull(s3Source.getKeyPrefix(), "source keyPrefix should be null");
        Assertions.assertTrue(s3Source.isIncludeVersions(), "source includeVersions should be enabled");

        Assertions.assertEquals(targetUri, s3Target.getUri(false), "target URI mismatch");
        Assertions.assertEquals(targetProtocol, s3Target.getProtocol(), "target protocol mismatch");
        Assertions.assertEquals(targetAccessKey, s3Target.getAccessKey(), "target access key mismatch");
        Assertions.assertEquals(targetSecret, s3Target.getSecretKey(), "target secret key mismatch");
        Assertions.assertEquals(targetHost, s3Target.getHost(), "target host mismatch");
        Assertions.assertEquals(targetPort, s3Target.getPort(), "target port mismatch");
        Assertions.assertEquals(targetBucket, s3Target.getBucketName(), "target bucket mismatch");
        Assertions.assertEquals(targetKeyPrefix, s3Target.getKeyPrefix(), "target keyPrefix mismatch");
        Assertions.assertTrue(s3Target.isCreateBucket(), "target createBucket should be enabled");
        Assertions.assertTrue(s3Target.isDisableVHosts(), "target disableVhost should be true");
        Assertions.assertTrue(s3Target.isIncludeVersions(), "target includeVersions should be enabled");
        Assertions.assertTrue(s3Target.isLegacySignatures(), "target legacySignatures should be enabled");
        Assertions.assertFalse(s3Target.isPreserveDirectories(), "target preserveDirectories should be disabled");
        Assertions.assertEquals(mpuThreshold, s3Target.getMpuThresholdMb(), "target MPU threshold mismatch");
        Assertions.assertEquals(mpuPartSize, s3Target.getMpuPartSizeMb(), "target MPU part size mismatch");
        Assertions.assertEquals(socketTimeout, s3Target.getSocketTimeoutMs(), "target socket timeout mismatch");
    }
}
