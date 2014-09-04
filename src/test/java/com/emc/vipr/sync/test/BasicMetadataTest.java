package com.emc.vipr.sync.test;

import com.emc.atmos.api.bean.Permission;
import com.emc.vipr.sync.model.BasicMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.Iso8601Util;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class BasicMetadataTest {
    @Test
    public void testJsonSerialization() {
        Map<String, String> userMeta = new HashMap<>();
        Map<String, String> sysMeta = new HashMap<>();
        Map<String, String> userAcl = new HashMap<>();
        Map<String, String> groupAcl = new HashMap<>();

        groupAcl.put("other", Permission.READ.toString());
        userAcl.put("stu", Permission.FULL_CONTROL.toString());
        userAcl.put("jason", Permission.NONE.toString());

        sysMeta.put("atime", "2013-01-14T21:51:53Z");
        sysMeta.put("mtime", "2013-01-14T21:51:53Z");
        sysMeta.put("ctime", "2013-01-14T22:07:31Z");
        sysMeta.put("itime", "2013-01-14T21:51:53Z");
        sysMeta.put("type", "regular");
        sysMeta.put("uid", "stu");
        sysMeta.put("gid", "apache");
        sysMeta.put("objectid", "50efd111a3068f64050f060bf69e40050f47df917176");
        sysMeta.put("objname", "2012 Atmos self-paced REST API training pptx.pptx");
        sysMeta.put("size", "210881");
        sysMeta.put("nlink", "1");
        sysMeta.put("policyname", "2LS_2YE_2YR");

        userMeta.put("foo", "bar");
        userMeta.put("baz", null);

        BasicMetadata metadata = new BasicMetadata();
        metadata.setSystemMetadata(sysMeta);
        metadata.setUserMetadata(userMeta);
        metadata.setUserAcl(userAcl);
        metadata.setGroupAcl(groupAcl);
        metadata.setContentType("application/ms-excel");
        metadata.setModifiedTime(new Date());
        metadata.setRetentionEnabled(true);
        metadata.setRetentionEndDate(new Date());
        metadata.setExpirationEnabled(true);
        metadata.setExpirationDate(new Date());

        String jsonString = metadata.toJson();

        BasicMetadata mFromJson = (BasicMetadata) BasicMetadata.fromJson(jsonString);
        Assert.assertEquals(metadata.getUserAcl(), mFromJson.getUserAcl());
        Assert.assertEquals(metadata.getGroupAcl(), mFromJson.getGroupAcl());
        Assert.assertEquals(metadata.getUserMetadata(), mFromJson.getUserMetadata());
        Assert.assertEquals(metadata.getSystemMetadata(), mFromJson.getSystemMetadata());
        Assert.assertEquals(metadata.getContentType(), mFromJson.getContentType());
        Assert.assertEquals(metadata.getModifiedTime(), mFromJson.getModifiedTime());
        Assert.assertEquals(metadata.isRetentionEnabled(), mFromJson.isRetentionEnabled());
        Assert.assertEquals(Iso8601Util.format(metadata.getRetentionEndDate()),
                Iso8601Util.format(mFromJson.getRetentionEndDate()));
        Assert.assertEquals(metadata.isExpirationEnabled(), mFromJson.isExpirationEnabled());
        Assert.assertEquals(Iso8601Util.format(metadata.getExpirationDate()),
                Iso8601Util.format(mFromJson.getExpirationDate()));
    }

    @Test
    public void testPathTranslation() {
        String sourcePath = "/foo/bar/baz.zip/dir";
        String metaPath = SyncMetadata.getMetaPath(sourcePath, true);
        Assert.assertEquals("dir failed", "/foo/bar/baz.zip/dir/.atmosmeta/.dirmeta", metaPath);

        sourcePath = "/foo/bar/baz.zip/dir/object";
        metaPath = SyncMetadata.getMetaPath(sourcePath, false);
        Assert.assertEquals("object failed", "/foo/bar/baz.zip/dir/.atmosmeta/object", metaPath);

        sourcePath = "foo/bar/baz/object";
        metaPath = SyncMetadata.getMetaPath(sourcePath, false);
        Assert.assertEquals("relative path failed", "foo/bar/baz/.atmosmeta/object", metaPath);
    }
}
