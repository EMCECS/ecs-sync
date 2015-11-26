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
package com.emc.ecs.sync;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.emc.ecs.sync.source.S3Source;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.S3Target;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.test.SyncConfig;
import com.emc.ecs.sync.test.TestObjectSource;
import com.emc.ecs.sync.test.TestSyncObject;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class VersionTest {
    private static final Logger log = LoggerFactory.getLogger(VersionTest.class);

    @Test
    public void testVersions() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        String bucket1 = "ecs-sync-s3-test-versions";
        String bucket2 = "ecs-sync-s3-test-versions-2";
        String endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        String accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        String secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        String proxyUri = syncProperties.getProperty(SyncConfig.PROP_HTTP_PROXY_URI);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);

        ClientConfiguration config = new ClientConfiguration().withSignerOverride("S3SignerType");
        if (proxyUri != null) {
            URI uri = new URI(proxyUri);
            config.setProxyHost(uri.getHost());
            config.setProxyPort(uri.getPort());
        }

        AmazonS3Client s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
        s3.setEndpoint(endpoint);

        log.info("creating buckets with versioning enabled...");
        s3.createBucket(bucket1);
        s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucket1,
                new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
        s3.createBucket(bucket2);
        s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucket2,
                new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));

        try {
            // create source data
            TestObjectSource testSource = new TestObjectSource(50, 10 * 1024, null);

            S3Target target = new S3Target();
            target.setEndpoint(endpoint);
            target.setAccessKey(accessKey);
            target.setSecretKey(secretKey);
            target.setBucketName(bucket1);
            target.setLegacySignatures(true);
            target.setForce(true);

            // push it into bucket1
            log.info("writing v1 source data...");
            runSync(testSource, target, false);

            // 2nd version is delete
            log.info("deleting source objects (for v2)...");
            deleteObjects(s3, bucket1);

            // 3rd version is altered
            log.info("writing v3 source data...");
            List<TestSyncObject> testDataV3 = alterContent(testSource.getObjects(), "3");
            testSource = new TestObjectSource(testDataV3);
            runSync(testSource, target, false);

            // 4th version is altered again
            log.info("writing v4 source data...");
            List<TestSyncObject> testDataV4 = alterContent(testDataV3, "4");
            testSource = new TestObjectSource(testDataV4);
            runSync(testSource, target, false);

            // now run migration to bucket2
            S3Source source = new S3Source();
            source.setEndpoint(endpoint);
            source.setAccessKey(accessKey);
            source.setSecretKey(secretKey);
            source.setBucketName(bucket1);
            source.setLegacySignatures(true);
            source.setIncludeAcl(true);

            target = new S3Target();
            target.setEndpoint(endpoint);
            target.setAccessKey(accessKey);
            target.setSecretKey(secretKey);
            target.setBucketName(bucket2);
            target.setLegacySignatures(true);
            target.setIncludeAcl(true);
            target.setCreateBucket(true);
            target.setIncludeVersions(true);

            log.info("migrating versions to bucket2...");
            runSync(source, target, false);

            // verify all versions
            log.info("verifying versions...");
            verifyBuckets(s3, bucket1, bucket2);

            // test verify only sync
            log.info("syncing with verify-only...");
            runSync(source, target, true);

            // add v5 (delete) to bucket1 (testing delete)
            log.info("deleting objects in source (for v5)...");
            deleteObjects(s3, bucket1);

            // test deleted objects
            log.info("migrating v5 to target...");
            runSync(source, target, false);
            log.info("verifying versions...");
            verifyBuckets(s3, bucket1, bucket2);

            // test deleted objects from scratch
            log.info("wiping bucket2...");
            deleteBucket(s3, bucket2);
            log.info("migrating versions again from scratch...");
            runSync(source, target, false);
            log.info("verifying versions...");
            verifyBuckets(s3, bucket1, bucket2);

            // test verify-only with deleted objects
            log.info("syncing with verify-only (all delete markers)...");
            runSync(source, target, true);

        } finally {
            log.info("cleaning up bucket1...");
            deleteBucket(s3, bucket1);
            log.info("cleaning up bucket2...");
            deleteBucket(s3, bucket2);
        }
    }

    protected void runSync(SyncSource source, SyncTarget target, boolean verifyOnly) {
        EcsSync sync = new EcsSync();
        sync.setSource(source);
        sync.setTarget(target);
        sync.setSyncThreadCount(32);
        sync.setRetryAttempts(0);
        if (verifyOnly) sync.setVerifyOnly(true);
        else sync.setVerify(true);

        sync.run();

        Assert.assertEquals(0, sync.getObjectsFailed());
    }

    protected List<TestSyncObject> alterContent(List<TestSyncObject> testData, String version) throws Exception {
        List<TestSyncObject> newTestData = new ArrayList<>();
        for (TestSyncObject object : testData) {
            TestSyncObject newObject = object.deepCopy();
            alterContent(newObject, version);
            newTestData.add(newObject);
        }
        return newTestData;
    }

    protected void alterContent(TestSyncObject object, String version) {
        if (object.getData() != null && object.getData().length > 0) object.getData()[0] += 1;
        object.getMetadata().setUserMetadataValue("version", version);
        if (object.getChildren() != null) {
            for (TestSyncObject child : object.getChildren()) {
                alterContent(child, version);
            }
        }
    }

    protected void verifyBuckets(AmazonS3 s3, String bucket1, String bucket2) {
        Map<String, SortedSet<S3VersionSummary>> map1 = getAllVersions(s3, bucket1);
        Map<String, SortedSet<S3VersionSummary>> map2 = getAllVersions(s3, bucket2);

        // should be sufficient to compare ETags since they should be valid
        Assert.assertEquals(map1.keySet(), map2.keySet());
        for (String key : map1.keySet()) {
            Assert.assertEquals(map1.get(key).size(), map2.get(key).size());
            Iterator<S3VersionSummary> summaries2 = map2.get(key).iterator();
            String lastEtag = null;
            for (S3VersionSummary summary1 : map1.get(key)) {
                S3VersionSummary summary2 = summaries2.next();
                Assert.assertEquals(summary1.isDeleteMarker(), summary2.isDeleteMarker());
                // probably sufficient to compare ETags
                Assert.assertEquals(summary1.getETag(), summary2.getETag());
                // also make sure it's different from the last ETag
                if (summary1.getSize() > 0) Assert.assertNotEquals(lastEtag, summary1.getETag());
                lastEtag = summary1.getETag();
            }
        }
    }

    protected Map<String, SortedSet<S3VersionSummary>> getAllVersions(AmazonS3 s3, String bucket) {
        // build comprehensive key -> versions map
        // must sort by keys and mtime for simple comparison
        Map<String, SortedSet<S3VersionSummary>> map = new TreeMap<>();

        VersionListing listing = null;
        do {
            if (listing == null) listing = s3.listVersions(bucket, null);
            else listing = s3.listNextBatchOfVersions(listing);

            for (S3VersionSummary summary : listing.getVersionSummaries()) {
                SortedSet<S3VersionSummary> versions = map.get(summary.getKey());
                if (versions == null) {
                    versions = new TreeSet<>(new VersionComparator());
                    map.put(summary.getKey(), versions);
                }
                versions.add(summary);
            }
        } while (listing.isTruncated());

        return map;
    }

    protected void deleteObjects(AmazonS3 s3, String bucket) {
        ObjectListing listing = null;
        do {
            if (listing == null) listing = s3.listObjects(bucket);
            else listing = s3.listNextBatchOfObjects(listing);

            List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
            for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
            }

            s3.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys));
        } while (listing.isTruncated());
    }

    protected void deleteBucket(final AmazonS3 s3, final String bucket) {
        ExecutorService executor = Executors.newFixedThreadPool(32);
        List<Future> futures = new ArrayList<>();
        try {
            VersionListing listing = null;
            do {
                if (listing == null) listing = s3.listVersions(bucket, null);
                else listing = s3.listNextBatchOfVersions(listing);

                for (final S3VersionSummary summary : listing.getVersionSummaries()) {
                    futures.add(executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            s3.deleteVersion(bucket, summary.getKey(), summary.getVersionId());
                        }
                    }));
                }
            } while (listing.isTruncated());

            for (Future future : futures) {
                try {
                    future.get();
                } catch (Throwable t) {
                    log.warn("error deleting version", t);
                }
            }

            s3.deleteBucket(bucket);
        } catch (RuntimeException e) {
            log.warn("could not delete bucket " + bucket, e);
        } finally {
            executor.shutdown();
        }
    }

    private class VersionComparator implements Comparator<S3VersionSummary> {
        @Override
        public int compare(S3VersionSummary o1, S3VersionSummary o2) {
            int result = o1.getLastModified().compareTo(o2.getLastModified());
            if (result == 0) result = o1.getVersionId().compareTo(o2.getVersionId());
            return result;
        }
    }
}
