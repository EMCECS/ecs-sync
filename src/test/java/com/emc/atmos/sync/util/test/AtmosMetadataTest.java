/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.atmos.sync.util.test;

import com.emc.atmos.api.Acl;
import com.emc.atmos.api.bean.Metadata;
import com.emc.atmos.api.bean.Permission;
import com.emc.atmos.sync.util.AtmosMetadata;
import com.emc.atmos.sync.util.Iso8601Util;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

public class AtmosMetadataTest {
    @Test
    public void testJsonSerialization() {
        Acl acl = new Acl();
        acl.addGroupGrant( "other", Permission.READ );
        acl.addUserGrant( "stu", Permission.FULL_CONTROL );
        acl.addUserGrant( "jason", Permission.NONE );

        Map<String, Metadata> sysMeta = new TreeMap<String, Metadata>();
        sysMeta.put( "atime", new Metadata( "atime", "2013-01-14T21:51:53Z", false ) );
        sysMeta.put( "mtime", new Metadata( "mtime", "2013-01-14T21:51:53Z", false ) );
        sysMeta.put( "ctime", new Metadata( "ctime", "2013-01-14T22:07:31Z", false ) );
        sysMeta.put( "itime", new Metadata( "itime", "2013-01-14T21:51:53Z", false ) );
        sysMeta.put( "type", new Metadata( "type", "regular", false ) );
        sysMeta.put( "uid", new Metadata( "uid", "stu", false ) );
        sysMeta.put( "gid", new Metadata( "gid", "apache", false ) );
        sysMeta.put( "objectid", new Metadata( "objectid", "50efd111a3068f64050f060bf69e40050f47df917176", false ) );
        sysMeta.put( "objname", new Metadata( "objname", "2012 Atmos self-paced REST API training pptx.pptx", false ) );
        sysMeta.put( "size", new Metadata( "size", "210881", false ) );
        sysMeta.put( "nlink", new Metadata( "nlink", "1", false ) );
        sysMeta.put( "policyname", new Metadata( "policyname", "2LS_2YE_2YR", false ) );

        Map<String, Metadata> userMeta = new TreeMap<String, Metadata>();
        userMeta.put( "foo", new Metadata( "foo", "bar", false ) );
        userMeta.put( "baz", new Metadata( "baz", null, true ) );

        AtmosMetadata atmosMetadata = new AtmosMetadata();
        atmosMetadata.setSystemMetadata( sysMeta );
        atmosMetadata.setMetadata( userMeta );
        atmosMetadata.setAcl( acl );
        atmosMetadata.setContentType( "application/ms-excel" );
        atmosMetadata.setRetentionEnabled( true );
        atmosMetadata.setRetentionEndDate( new Date() );
        atmosMetadata.setExpirationEnabled( true );
        atmosMetadata.setExpirationDate( new Date() );

        String jsonString = atmosMetadata.toJson();

        AtmosMetadata amFromJson = AtmosMetadata.fromJson( jsonString );
        Assert.assertEquals( atmosMetadata.getAcl(), amFromJson.getAcl() );
        Assert.assertEquals( atmosMetadata.getMetadata(), amFromJson.getMetadata() );
        Assert.assertEquals( atmosMetadata.getSystemMetadata(), amFromJson.getSystemMetadata() );
        Assert.assertEquals( atmosMetadata.getContentType(), amFromJson.getContentType() );
        Assert.assertEquals( atmosMetadata.isRetentionEnabled(), amFromJson.isRetentionEnabled() );
        Assert.assertEquals( Iso8601Util.format( atmosMetadata.getRetentionEndDate() ),
                             Iso8601Util.format( amFromJson.getRetentionEndDate() ) );
        Assert.assertEquals( atmosMetadata.isExpirationEnabled(), amFromJson.isExpirationEnabled() );
        Assert.assertEquals( Iso8601Util.format( atmosMetadata.getExpirationDate() ),
                             Iso8601Util.format( amFromJson.getExpirationDate() ) );
    }
}
