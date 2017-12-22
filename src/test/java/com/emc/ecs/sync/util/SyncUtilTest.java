/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
