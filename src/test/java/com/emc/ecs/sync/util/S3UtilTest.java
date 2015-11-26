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
package com.emc.ecs.sync.util;

import org.junit.Assert;
import org.junit.Test;

public class S3UtilTest {
    @Test
    public void testAwsS3Uri() {
        String rawUri = "s3:http://user:pass@s3.company.com:8080/foo";
        AwsS3Util.S3Uri s3Uri = AwsS3Util.parseUri(rawUri);
        Assert.assertEquals("http", s3Uri.protocol);
        Assert.assertEquals("s3.company.com:8080", s3Uri.endpoint);
        Assert.assertEquals("user", s3Uri.accessKey);
        Assert.assertEquals("pass", s3Uri.secretKey);
        Assert.assertEquals("foo", s3Uri.rootKey);
        Assert.assertEquals(rawUri, s3Uri.toUri());

        rawUri = "s3:user:s3cr37k3y";
        s3Uri = AwsS3Util.parseUri(rawUri);
        Assert.assertNull(s3Uri.protocol);
        Assert.assertNull(s3Uri.endpoint);
        Assert.assertEquals("user", s3Uri.accessKey);
        Assert.assertEquals("s3cr37k3y", s3Uri.secretKey);
        Assert.assertNull(s3Uri.rootKey);
        Assert.assertEquals(rawUri, s3Uri.toUri());

        rawUri = "s3:user:s3cr37k3y@/foo";
        s3Uri = AwsS3Util.parseUri(rawUri);
        Assert.assertNull(s3Uri.protocol);
        Assert.assertNull(s3Uri.endpoint);
        Assert.assertEquals("user", s3Uri.accessKey);
        Assert.assertEquals("s3cr37k3y", s3Uri.secretKey);
        Assert.assertEquals("foo", s3Uri.rootKey);
        Assert.assertEquals(rawUri, s3Uri.toUri());

        rawUri = "s3:user:s3cr37k3y@s3.company.com";
        s3Uri = AwsS3Util.parseUri(rawUri);
        Assert.assertNull(s3Uri.protocol);
        Assert.assertEquals("s3.company.com", s3Uri.endpoint);
        Assert.assertEquals("user", s3Uri.accessKey);
        Assert.assertEquals("s3cr37k3y", s3Uri.secretKey);
        Assert.assertNull(s3Uri.rootKey);
        Assert.assertEquals(rawUri, s3Uri.toUri());
    }

    @Test
    public void testEcsS3Uri() {
        String rawUri = "ecs-s3:http://user:pass@s3.company.com:8080/foo";
        EcsS3Util.S3Uri s3Uri = EcsS3Util.parseUri(rawUri);
        Assert.assertEquals("http", s3Uri.protocol);
        Assert.assertEquals("http://s3.company.com:8080", s3Uri.getEndpointUri().toString());
        Assert.assertEquals(1, s3Uri.vdcs.size());
        Assert.assertEquals(1, s3Uri.vdcs.get(0).getHosts().size());
        Assert.assertEquals("s3.company.com", s3Uri.vdcs.get(0).getHosts().get(0).getName());
        Assert.assertEquals(8080, s3Uri.port);
        Assert.assertEquals("user", s3Uri.accessKey);
        Assert.assertEquals("pass", s3Uri.secretKey);
        Assert.assertEquals("foo", s3Uri.rootKey);
        Assert.assertEquals(rawUri, s3Uri.toUri());

        rawUri = "ecs-s3:http://user:pass@vdc1(1.1.1.11,1.1.1.12),vdc2(1.1.2.11,1.1.2.12):9020/foo";
        s3Uri = EcsS3Util.parseUri(rawUri);
        Assert.assertEquals("http", s3Uri.protocol);
        Assert.assertEquals("http://1.1.1.11:9020", s3Uri.getEndpointUri().toString());
        Assert.assertEquals(2, s3Uri.vdcs.size());
        Assert.assertEquals(2, s3Uri.vdcs.get(0).getHosts().size());
        Assert.assertEquals(2, s3Uri.vdcs.get(1).getHosts().size());
        Assert.assertEquals("1.1.1.11", s3Uri.vdcs.get(0).getHosts().get(0).getName());
        Assert.assertEquals("1.1.1.12", s3Uri.vdcs.get(0).getHosts().get(1).getName());
        Assert.assertEquals("1.1.2.11", s3Uri.vdcs.get(1).getHosts().get(0).getName());
        Assert.assertEquals("1.1.2.12", s3Uri.vdcs.get(1).getHosts().get(1).getName());
        Assert.assertEquals(9020, s3Uri.port);
        Assert.assertEquals("user", s3Uri.accessKey);
        Assert.assertEquals("pass", s3Uri.secretKey);
        Assert.assertEquals("foo", s3Uri.rootKey);
        Assert.assertEquals(rawUri, s3Uri.toUri());
    }
}
