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
package com.emc.vipr.sync.test;

import com.emc.atmos.api.Acl;
import com.emc.atmos.api.bean.Permission;
import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.Iso8601Util;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import static com.emc.vipr.sync.model.SyncMetadata.UserMetadata;

public class AtmosMetadataTest {
    @Test
    public void testJsonSerialization() throws Exception {
        Acl acl = new Acl();
        acl.addGroupGrant("other", Permission.READ);
        acl.addUserGrant("stu", Permission.FULL_CONTROL);
        acl.addUserGrant("jason", Permission.NONE);

        Map<String, UserMetadata> sysMeta = new TreeMap<>();
        sysMeta.put("atime", new UserMetadata("atime", "2013-01-14T21:51:53Z", false));
        sysMeta.put("mtime", new UserMetadata("mtime", "2013-01-14T21:51:53Z", false));
        sysMeta.put("ctime", new UserMetadata("ctime", "2013-01-14T22:07:31Z", false));
        sysMeta.put("itime", new UserMetadata("itime", "2013-01-14T21:51:53Z", false));
        sysMeta.put("type", new UserMetadata("type", "regular", false));
        sysMeta.put("uid", new UserMetadata("uid", "stu", false));
        sysMeta.put("gid", new UserMetadata("gid", "apache", false));
        sysMeta.put("objectid", new UserMetadata("objectid", "50efd111a3068f64050f060bf69e40050f47df917176", false));
        sysMeta.put("objname", new UserMetadata("objname", "2012 Atmos self-paced REST API training pptx.pptx", false));
        sysMeta.put("size", new UserMetadata("size", "210881", false));
        sysMeta.put("nlink", new UserMetadata("nlink", "1", false));
        sysMeta.put("policyname", new UserMetadata("policyname", "2LS_2YE_2YR", false));

        Map<String, UserMetadata> userMeta = new TreeMap<>();
        userMeta.put("foo", new UserMetadata("foo", "bar", false));
        userMeta.put("baz", new UserMetadata("baz", null, true));

        AtmosMetadata atmosMetadata = new AtmosMetadata();
        atmosMetadata.setSystemMetadata(sysMeta);
        atmosMetadata.setUserMetadata(userMeta);
        atmosMetadata.setAcl(AtmosMetadata.syncAclFromAtmosAcl(acl));
        atmosMetadata.setContentType("application/ms-excel");
        atmosMetadata.setRetentionEnabled(true);
        atmosMetadata.setRetentionEndDate(new Date());
        atmosMetadata.setExpirationDate(new Date());

        String jsonString = atmosMetadata.toJson();

        SyncMetadata mFromJson = SyncMetadata.fromJson(jsonString);
        Assert.assertTrue(mFromJson instanceof AtmosMetadata);
        AtmosMetadata amFromJson = (AtmosMetadata) mFromJson;
        Assert.assertEquals(atmosMetadata.getAcl(), amFromJson.getAcl());
        Assert.assertEquals(atmosMetadata.getUserMetadata(), amFromJson.getUserMetadata());
        Assert.assertEquals(atmosMetadata.getSystemMetadata(), amFromJson.getSystemMetadata());
        Assert.assertEquals(atmosMetadata.getContentType(), amFromJson.getContentType());
        Assert.assertEquals(atmosMetadata.isRetentionEnabled(), amFromJson.isRetentionEnabled());
        Assert.assertEquals(Iso8601Util.format(atmosMetadata.getRetentionEndDate()),
                Iso8601Util.format(amFromJson.getRetentionEndDate()));
        Assert.assertEquals(Iso8601Util.format(atmosMetadata.getExpirationDate()),
                Iso8601Util.format(amFromJson.getExpirationDate()));
    }
}
