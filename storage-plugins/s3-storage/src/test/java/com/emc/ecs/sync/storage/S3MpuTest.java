/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.EcsS3Config;
import com.emc.ecs.sync.storage.s3.AbstractS3Test;
import com.emc.ecs.sync.test.StartNotifyFilter;
import com.emc.ecs.sync.util.RandomInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class S3MpuTest {
    private static final Logger log = LoggerFactory.getLogger(S3MpuTest.class);

    String sourceBucket = "ecs-sync-s3-mpu-test-bucket-source";
    String targetBucket = "ecs-sync-s3-mpu-test-bucket-target";
    AmazonS3 s3;

    @BeforeEach
    public void setup() {
        s3 = AbstractS3Test.createS3Client();
        AbstractS3Test.createBucket(s3, sourceBucket, false);
        AbstractS3Test.createBucket(s3, targetBucket, false);
    }

    @AfterEach
    public void teardown() {
        if (s3 != null) {
            AbstractS3Test.deleteBucket(s3, sourceBucket);
            AbstractS3Test.deleteBucket(s3, targetBucket);
            s3.shutdown();
        }
    }

    // tests copying a large MPU from ECS to AWS and terminating the job early (simulates Data Movement scan job timeout)
    @Test
    public void testMpuTerminateEarly() throws Exception {
        String key = "mpu-test-terminate-early";
        int sizeMb = 128;
        long size = sizeMb * 1024 * 1024;
        int bandwidthLimit = 10 * 1024 * 1024; // 10 MB/s

        EcsS3Config ecsConfig = new EcsS3Test().generateConfig(sourceBucket);
        ecsConfig.setBucketName(sourceBucket);
        AwsS3Config awsConfig = new AwsS3Test().generateConfig(targetBucket);
        awsConfig.setBucketName(targetBucket);
        // tweak MPU settings for the test - make sure when we pause, the in-progress parts won't complete the whole object
        awsConfig.setMpuThresholdMb(sizeMb);
        awsConfig.setMpuPartSizeMb(1); // this will set it to the minimum size
        awsConfig.setMpuResumeEnabled(true);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(size);

        // first, write large object to source bucket
        s3.putObject(sourceBucket, key, new RandomInputStream(size), metadata);

        // use sync job to MPU to target bucket
        SyncConfig syncConfig = new SyncConfig().withSource(ecsConfig).withTarget(awsConfig);
        // lower thread count to lower in-progress parts when we terminate (again, make sure we don't complete the object)
        // also add a throttle, so we don't complete really quickly in local lab environments
        syncConfig.withOptions(new SyncOptions().withThreadCount(8).withBandwidthLimit(bandwidthLimit));
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        StartNotifyFilter.StartNotifyConfig notifyConfig = new StartNotifyFilter.StartNotifyConfig();
        syncConfig.withFilters(Collections.singletonList(notifyConfig));

        CompletableFuture<?> future = CompletableFuture.runAsync(sync);
        notifyConfig.waitForStart();
        Thread.sleep(2000); // need to terminate after the first parts are started, but before they finish
        sync.terminate();
        future.get();

        // object should not exist
        Assertions.assertFalse(s3.doesObjectExist(targetBucket, key));
        // MPU should exist with at least 1 part
        List<MultipartUpload> uploads = s3.listMultipartUploads(new ListMultipartUploadsRequest(targetBucket)).getMultipartUploads();
        Assertions.assertEquals(1, uploads.size());
        Assertions.assertTrue(s3.listParts(new ListPartsRequest(targetBucket, key, uploads.get(0).getUploadId())).getParts().size() > 0);
    }

    @Test
    public void testBWThrottleMpu() throws Exception {
        String key = "mpu-throttle-test";
        int sizeMb = 40;
        long size = sizeMb * 1024 * 1024;
        int bandwidthLimit = 2 * 1024 * 1024; // 2 MiB/s

        EcsS3Config ecsConfig = new EcsS3Test().generateConfig(sourceBucket);
        ecsConfig.setBucketName(sourceBucket);
        AwsS3Config awsConfig = new AwsS3Test().generateConfig(targetBucket);
        awsConfig.setBucketName(targetBucket);
        // tweak MPU settings for the test - make sure when we pause, the in-progress parts won't complete the whole object
        awsConfig.setMpuThresholdMb(sizeMb);
        awsConfig.setMpuPartSizeMb(1); // this will set it to the minimum size

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(size);

        // first, write large object to source bucket
        s3.putObject(sourceBucket, key, new RandomInputStream(size), metadata);

        // use sync job to MPU to target bucket
        SyncConfig syncConfig = new SyncConfig().withSource(ecsConfig).withTarget(awsConfig);
        // lower thread count to lower in-progress parts when we terminate (again, make sure we don't complete the object)
        syncConfig.withOptions(new SyncOptions().withThreadCount(8).withBandwidthLimit(bandwidthLimit));
        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        StartNotifyFilter.StartNotifyConfig notifyConfig = new StartNotifyFilter.StartNotifyConfig();
        syncConfig.withFilters(Collections.singletonList(notifyConfig));

        CompletableFuture<?> future = CompletableFuture.runAsync(sync);
        notifyConfig.waitForStart();

        long startTime = System.currentTimeMillis();
        future.join();
        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        long bwRate = sync.getStats().getBytesComplete() * 1000 / durationMs; // adjust to bytes/second

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed());
        Assertions.assertEquals(1, sync.getStats().getObjectsComplete());
        log.warn("BW limit: {}, actual rate: {}", bandwidthLimit, bwRate);
        Assertions.assertEquals(bandwidthLimit, bwRate, bandwidthLimit * 0.1); // 10% margin of error
    }
}