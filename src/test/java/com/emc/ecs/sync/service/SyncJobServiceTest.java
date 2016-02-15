/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.CommonOptions;
import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.rest.PluginConfig;
import com.emc.ecs.sync.rest.SyncConfig;
import com.emc.ecs.sync.filter.IdLoggingFilter;
import com.emc.ecs.sync.filter.OverrideMimetypeFilter;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.source.S3Source;
import com.emc.ecs.sync.target.S3Target;
import org.junit.Assert;
import org.junit.Test;

public class SyncJobServiceTest {
    @Test
    public void testConfigureSync() throws Exception {
        int queryThreadCount = 4;
        int syncThreadCount = 4;
        boolean recursive = false;
        boolean timingsEnabled = true;
        int timingWindow = 20000;
        boolean rememberFailed = false;
        boolean verify = true;
        boolean verifyOnly = true;
        boolean deleteSource = true;
        String logLevel = "verbose";

        boolean metadataOnly = true;
        boolean ignoreMetadata = true;
        boolean includeAcl = true;
        boolean ignoreInvalidAcls = true;
        boolean includeRetentionExpiration = true;
        boolean force = true;
        int bufferSize = CommonOptions.DEFAULT_BUFFER_SIZE + 1;

        String sourceBucket = "source-bucket";
        String sourceRootKey = "source/prefix/";
        String protocol = "http";
        String endpoint = "10.249.237.104";
        String accessKey = "wuser1@SANITY.LOCAL";
        String secretKey = "awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi";
        boolean disableVHosts = true;
        boolean decodeKeys = true;
        boolean legacySignatures = true;

        String targetBucket = "target-bucket";
        String targetRootKey = "target/prefix/";
        boolean createBucket = true;
        boolean includeVersions = true;
        int mpuThresholdMB = 5;
        int mpuPartSizeMB = 5;
        int mpuThreadCount = 3;

        String filename = "/tmp/foo.log";
        String mimeType = "application/foo";
        boolean mimeForce = true;

        SyncConfig config = new SyncConfig();
        config.setSource(new PluginConfig(S3Source.class.getName())
                .addCustomProperty("protocol", protocol)
                .addCustomProperty("endpoint", endpoint)
                .addCustomProperty("accessKey", accessKey)
                .addCustomProperty("secretKey", secretKey)
                .addCustomProperty("disableVHosts", "" + disableVHosts)
                .addCustomProperty("bucketName", sourceBucket)
                .addCustomProperty("rootKey", sourceRootKey)
                .addCustomProperty("decodeKeys", "" + decodeKeys)
                .addCustomProperty("legacySignatures", "" + legacySignatures));
        config.setTarget(new PluginConfig(S3Target.class.getName())
                .addCustomProperty("protocol", protocol)
                .addCustomProperty("endpoint", endpoint)
                .addCustomProperty("accessKey", accessKey)
                .addCustomProperty("secretKey", secretKey)
                .addCustomProperty("disableVHosts", "" + disableVHosts)
                .addCustomProperty("bucketName", targetBucket)
                .addCustomProperty("rootKey", targetRootKey)
                .addCustomProperty("createBucket", "" + createBucket)
                .addCustomProperty("includeVersions", "" + includeVersions)
                .addCustomProperty("legacySignatures", "" + legacySignatures)
                .addCustomProperty("mpuThresholdMB", "" + mpuThresholdMB)
                .addCustomProperty("mpuPartSizeMB", "" + mpuPartSizeMB)
                .addCustomProperty("mpuThreadCount", "" + mpuThreadCount));
        config.getFilters().add(new PluginConfig(IdLoggingFilter.class.getName())
                .addCustomProperty("filename", filename));
        config.getFilters().add(new PluginConfig(OverrideMimetypeFilter.class.getName())
                .addCustomProperty("mimeType", mimeType)
                .addCustomProperty("force", "" + mimeForce));

        config.setQueryThreadCount(queryThreadCount);
        config.setSyncThreadCount(syncThreadCount);
        config.setRecursive(recursive);
        config.setTimingsEnabled(timingsEnabled);
        config.setTimingWindow(timingWindow);
        config.setRememberFailed(rememberFailed);
        config.setVerify(verify);
        config.setVerifyOnly(verifyOnly);
        config.setDeleteSource(deleteSource);
        config.setLogLevel(logLevel);

        config.setMetadataOnly(metadataOnly);
        config.setIgnoreMetadata(ignoreMetadata);
        config.setIncludeAcl(includeAcl);
        config.setIgnoreInvalidAcls(ignoreInvalidAcls);
        config.setIncludeRetentionExpiration(includeRetentionExpiration);
        config.setForce(force);
        config.setBufferSize(bufferSize);

        EcsSync sync = new EcsSync();
        SyncJobService.getInstance().configureSync(sync, config);

        Assert.assertNotNull(sync.getSource());
        Assert.assertEquals(S3Source.class, sync.getSource().getClass());
        S3Source s3Source = (S3Source) sync.getSource();
        Assert.assertEquals(protocol, s3Source.getProtocol());
        Assert.assertEquals(endpoint, s3Source.getEndpoint());
        Assert.assertEquals(accessKey, s3Source.getAccessKey());
        Assert.assertEquals(secretKey, s3Source.getSecretKey());
        Assert.assertEquals(disableVHosts, s3Source.isDisableVHosts());
        Assert.assertEquals(sourceBucket, s3Source.getBucketName());
        Assert.assertEquals(sourceRootKey, s3Source.getRootKey());
        Assert.assertEquals(decodeKeys, s3Source.isDecodeKeys());
        Assert.assertEquals(legacySignatures, s3Source.isLegacySignatures());
        verifyPluginCommonOptions(s3Source, config);

        Assert.assertNotNull(sync.getTarget());
        Assert.assertEquals(S3Target.class, sync.getTarget().getClass());
        S3Target s3Target = (S3Target) sync.getTarget();
        Assert.assertEquals(protocol, s3Target.getProtocol());
        Assert.assertEquals(endpoint, s3Target.getEndpoint());
        Assert.assertEquals(accessKey, s3Target.getAccessKey());
        Assert.assertEquals(secretKey, s3Target.getSecretKey());
        Assert.assertEquals(disableVHosts, s3Target.isDisableVHosts());
        Assert.assertEquals(targetBucket, s3Target.getBucketName());
        Assert.assertEquals(targetRootKey, s3Target.getRootKey());
        Assert.assertEquals(createBucket, s3Target.isCreateBucket());
        Assert.assertEquals(includeVersions, s3Target.isIncludeVersions());
        Assert.assertEquals(legacySignatures, s3Target.isLegacySignatures());
        Assert.assertEquals(mpuThresholdMB, s3Target.getMpuThresholdMB());
        Assert.assertEquals(mpuPartSizeMB, s3Target.getMpuPartSizeMB());
        Assert.assertEquals(mpuThreadCount, s3Target.getMpuThreadCount());
        verifyPluginCommonOptions(s3Target, config);

        Assert.assertEquals(2, sync.getFilters().size());

        SyncFilter filter = sync.getFilters().get(0);
        Assert.assertEquals(IdLoggingFilter.class, filter.getClass());
        IdLoggingFilter idLoggingFilter = (IdLoggingFilter) filter;
        Assert.assertEquals(filename, idLoggingFilter.getFilename());
        verifyPluginCommonOptions(filter, config);

        filter = sync.getFilters().get(1);
        Assert.assertEquals(OverrideMimetypeFilter.class, filter.getClass());
        OverrideMimetypeFilter overrideMimetypeFilter = (OverrideMimetypeFilter) filter;
        Assert.assertEquals(mimeType, overrideMimetypeFilter.getMimeType());
        Assert.assertEquals(mimeForce, overrideMimetypeFilter.isForce());
        verifyPluginCommonOptions(filter, config);

        Assert.assertEquals(queryThreadCount, sync.getQueryThreadCount());
        Assert.assertEquals(syncThreadCount, sync.getSyncThreadCount());
        Assert.assertEquals(recursive, sync.isRecursive());
        Assert.assertEquals(timingsEnabled, sync.isTimingsEnabled());
        Assert.assertEquals(timingWindow, sync.getTimingWindow());
        Assert.assertEquals(rememberFailed, sync.isRememberFailed());
        Assert.assertEquals(verify, sync.isVerify());
        Assert.assertEquals(verifyOnly, sync.isVerifyOnly());
        Assert.assertEquals(deleteSource, sync.isDeleteSource());
        Assert.assertEquals(logLevel, sync.getLogLevel());
    }

    private void verifyPluginCommonOptions(SyncPlugin plugin, SyncConfig config) {
        Assert.assertEquals(config.getBufferSize().longValue(), plugin.getBufferSize());
        Assert.assertEquals(config.getMetadataOnly().booleanValue(), plugin.isMetadataOnly());
        Assert.assertEquals(config.getIgnoreMetadata().booleanValue(), plugin.isIgnoreMetadata());
        Assert.assertEquals(config.getIncludeAcl().booleanValue(), plugin.isIncludeAcl());
        Assert.assertEquals(config.getIgnoreInvalidAcls().booleanValue(), plugin.isIgnoreInvalidAcls());
        Assert.assertEquals(config.getIncludeRetentionExpiration().booleanValue(), plugin.isIncludeRetentionExpiration());
        Assert.assertEquals(config.getForce().booleanValue(), plugin.isForce());
    }
}
