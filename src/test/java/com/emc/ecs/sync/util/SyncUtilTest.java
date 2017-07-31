package com.emc.ecs.sync.util;

import org.junit.Assert;
import org.junit.Test;

public class SyncUtilTest {
    @Test
    public void testCombinedPath() {
        Assert.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo", "bar"));
        Assert.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo/", "bar"));
        Assert.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo", "/bar"));
        Assert.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo/", "/bar"));
        Assert.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo", "bar"));
        Assert.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo\\", "bar"));
        Assert.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo", "\\bar"));
        Assert.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo\\", "\\bar"));
        Assert.assertEquals("/foo/./bar", SyncUtil.combinedPath("/foo/.", "bar"));
        Assert.assertEquals("/foo/../bar", SyncUtil.combinedPath("/foo/..", "bar"));
        Assert.assertEquals("/foo", SyncUtil.combinedPath("/foo", ""));
        Assert.assertEquals("/foo", SyncUtil.combinedPath("/foo", " "));
        Assert.assertEquals("/foo", SyncUtil.combinedPath("/foo", null));
        Assert.assertEquals("/bar", SyncUtil.combinedPath(null, "/bar"));
        Assert.assertEquals("/bar", SyncUtil.combinedPath(" ", "/bar"));
        Assert.assertEquals("\\bar", SyncUtil.combinedPath("", "\\bar"));
    }

    @Test
    public void testParentPath() {
        Assert.assertEquals("/foo", SyncUtil.parentPath("/foo/bar"));
        Assert.assertEquals("/foo", SyncUtil.parentPath("/foo/bar/"));
        Assert.assertEquals("/foo", SyncUtil.parentPath("/foo/."));
        Assert.assertEquals("/foo", SyncUtil.parentPath("/foo/../"));
        Assert.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\bar"));
        Assert.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\bar\\"));
        Assert.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\.\\"));
        Assert.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\.."));
        Assert.assertEquals("/", SyncUtil.parentPath("/foo"));
        Assert.assertEquals("/", SyncUtil.parentPath("/foo/"));
        Assert.assertEquals("/", SyncUtil.parentPath("/."));
        Assert.assertEquals("/", SyncUtil.parentPath("/../"));
        Assert.assertEquals("\\", SyncUtil.parentPath("\\foo"));
        Assert.assertEquals("\\", SyncUtil.parentPath("\\foo\\"));
        Assert.assertEquals("\\", SyncUtil.parentPath("\\.\\"));
        Assert.assertEquals("\\", SyncUtil.parentPath("\\.."));
        Assert.assertNull(SyncUtil.parentPath("/"));
        Assert.assertNull(SyncUtil.parentPath("\\"));
        Assert.assertNull(SyncUtil.parentPath(""));
        Assert.assertNull(SyncUtil.parentPath(null));
    }
}
