/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.emc.vipr.sync.source.S3Source;
import com.emc.vipr.sync.target.S3Target;
import com.emc.vipr.sync.test.SyncConfig;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.*;

public class S3AclTest {
    private static final Logger l4j = Logger.getLogger(S3AclTest.class);

    private String endpoint;
    private String accessKey;
    private String secretKey;
    private AmazonS3 s3;

    @Before
    public void setup() throws Exception {
        Properties syncProperties = SyncConfig.getProperties();
        endpoint = syncProperties.getProperty(SyncConfig.PROP_S3_ENDPOINT);
        accessKey = syncProperties.getProperty(SyncConfig.PROP_S3_ACCESS_KEY_ID);
        secretKey = syncProperties.getProperty(SyncConfig.PROP_S3_SECRET_KEY);
        Assume.assumeNotNull(endpoint, accessKey, secretKey);

        s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
        s3.setEndpoint(endpoint);
    }

    @Test
    public void testSetAcl() throws Exception {
        String bucket = "vipr-sync-s3-test-acl";
        String key = "test-object";
        createBucket(bucket, true);

        try {
            String content = "hello ACLs";

            s3.putObject(bucket, key, new ByteArrayInputStream(content.getBytes()), null); // 1st version

            AccessControlList acl = new AccessControlList();
            acl.setOwner(new Owner(accessKey, accessKey));
            acl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);
            acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
            acl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
            acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);

            PutObjectRequest putRequest = new PutObjectRequest(bucket, key, new ByteArrayInputStream(content.getBytes()), null);
            putRequest.setAccessControlList(acl);

            s3.putObject(putRequest); // 2nd version

            AccessControlList remoteAcl = s3.getObjectAcl(bucket, key);

            verifyAcls(acl, remoteAcl);
        } finally {
            try {
                deleteBucket(bucket);
            } catch (Throwable t) {
                l4j.warn("could not delete bucket: " + t.getMessage());
            }
        }
    }

    @Test
    public void testSyncWithAcls() throws Exception {
        String bucket1 = "vipr-sync-s3-test-sync-acl1";
        String bucket2 = "vipr-sync-s3-test-sync-acl2";
        createBucket(bucket1, true);

        String key1 = "key1", key2 = "key2", key3 = "key3";

        AccessControlList largeAcl = new AccessControlList();
        largeAcl.setOwner(new Owner(accessKey, accessKey));
        largeAcl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);
        largeAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);
        largeAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Write);
        largeAcl.grantPermission(GroupGrantee.AllUsers, Permission.Read);

        AccessControlList midAcl = new AccessControlList();
        midAcl.setOwner(new Owner(accessKey, accessKey));
        midAcl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);
        midAcl.grantPermission(GroupGrantee.AuthenticatedUsers, Permission.Read);

        AccessControlList defaultAcl = new AccessControlList();
        defaultAcl.setOwner(new Owner(accessKey, accessKey));
        defaultAcl.grantPermission(new CanonicalGrantee(accessKey), Permission.FullControl);

        try {
            // default acls
            s3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);
            s3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);
            s3.putObject(bucket1, key1, new ByteArrayInputStream("data1".getBytes()), null);

            // default acl on latest
            PutObjectRequest request = new PutObjectRequest(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);
            request.setAccessControlList(largeAcl);
            s3.putObject(request);
            request = new PutObjectRequest(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);
            request.setAccessControlList(midAcl);
            s3.putObject(request);
            s3.putObject(bucket1, key2, new ByteArrayInputStream("data2".getBytes()), null);

            // default acl on first version
            s3.putObject(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request = new PutObjectRequest(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request.setAccessControlList(midAcl);
            s3.putObject(request);
            request = new PutObjectRequest(bucket1, key3, new ByteArrayInputStream("data3".getBytes()), null);
            request.setAccessControlList(largeAcl);
            s3.putObject(request);

            S3Source source = new S3Source();
            source.setEndpoint(endpoint);
            source.setAccessKey(accessKey);
            source.setSecretKey(secretKey);
            source.setBucketName(bucket1);
            source.setIncludeAcl(true);

            S3Target target = new S3Target();
            target.setEndpoint(endpoint);
            target.setAccessKey(accessKey);
            target.setSecretKey(secretKey);
            target.setBucketName(bucket2);
            target.setIncludeAcl(true);
            target.setCreateBucket(true);
            target.setIncludeVersions(true);

            ViPRSync sync = new ViPRSync();
            sync.setSource(source);
            sync.setTarget(target);
            sync.setSyncThreadCount(1);
            sync.setVerify(true);
            sync.run();

            Assert.assertEquals(0, sync.getFailedCount());

            List<S3VersionSummary> key1Versions = getVersions(bucket2, key1);
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key1, key1Versions.get(0).getVersionId()));
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key1, key1Versions.get(1).getVersionId()));
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key1, key1Versions.get(2).getVersionId()));

            List<S3VersionSummary> key2Versions = getVersions(bucket2, key2);
            verifyAcls(largeAcl, s3.getObjectAcl(bucket2, key2, key2Versions.get(0).getVersionId()));
            verifyAcls(midAcl, s3.getObjectAcl(bucket2, key2, key2Versions.get(1).getVersionId()));
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key2, key2Versions.get(2).getVersionId()));

            List<S3VersionSummary> key3Versions = getVersions(bucket2, key3);
            verifyAcls(defaultAcl, s3.getObjectAcl(bucket2, key3, key3Versions.get(0).getVersionId()));
            verifyAcls(midAcl, s3.getObjectAcl(bucket2, key3, key3Versions.get(1).getVersionId()));
            verifyAcls(largeAcl, s3.getObjectAcl(bucket2, key3, key3Versions.get(2).getVersionId()));
        } finally {
            deleteBucket(bucket1);
            deleteBucket(bucket2);
        }
    }

    protected void verifyAcls(AccessControlList acl1, AccessControlList acl2) {
        Assert.assertEquals(acl1.getOwner(), acl2.getOwner());
        for (Grant grant : acl1.getGrants()) {
            Assert.assertTrue(acl2.getGrants().contains(grant));
        }
        for (Grant grant : acl2.getGrants()) {
            Assert.assertTrue(acl1.getGrants().contains(grant));
        }
    }

    protected List<S3VersionSummary> getVersions(String bucket, String key) {
        List<S3VersionSummary> summaries = new ArrayList<S3VersionSummary>();

        VersionListing listing = null;
        do {
            if (listing == null) listing = s3.listVersions(bucket, key);
            else listing = s3.listNextBatchOfVersions(listing);

            for (S3VersionSummary summary : listing.getVersionSummaries()) {
                if (summary.getKey().equals(key)) summaries.add(summary);
            }
        } while (listing.isTruncated());

        Collections.sort(summaries, new VersionComparator());

        return summaries;
    }

    private void createBucket(String bucket, boolean withVersioning) {
        try {
            s3.createBucket(bucket);
        } catch (AmazonServiceException e) {
            if (!e.getErrorCode().equals("BucketAlreadyExists")) throw e;
        }

        if (withVersioning) {
            s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucket,
                    new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED)));
        }
    }

    private void deleteBucket(String bucket) {
        try {
            VersionListing listing = null;
            do {
                if (listing == null) listing = s3.listVersions(bucket, null);
                else listing = s3.listNextBatchOfVersions(listing);

                List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<DeleteObjectsRequest.KeyVersion>();
                for (S3VersionSummary summary : listing.getVersionSummaries()) {
                    keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey(), summary.getVersionId()));
                }
                s3.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys));
            } while (listing.isTruncated());

            s3.deleteBucket(bucket);
        } catch (RuntimeException e) {
            l4j.warn("could not delete bucket " + bucket, e);
        }
    }

    private class VersionComparator implements Comparator<S3VersionSummary> {
        @Override
        public int compare(S3VersionSummary o1, S3VersionSummary o2) {
            return o1.getLastModified().compareTo(o2.getLastModified());
        }
    }
}
