/*
 * Copyright (c) 2017-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SyncUtilTest {
    @Test
    public void testCombinedPath() {
        Assertions.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo", "bar"));
        Assertions.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo/", "bar"));
        Assertions.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo", "/bar"));
        Assertions.assertEquals("/foo/bar", SyncUtil.combinedPath("/foo/", "/bar"));
        Assertions.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo", "bar"));
        Assertions.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo\\", "bar"));
        Assertions.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo", "\\bar"));
        Assertions.assertEquals("\\foo/bar", SyncUtil.combinedPath("\\foo\\", "\\bar"));
        Assertions.assertEquals("/foo/./bar", SyncUtil.combinedPath("/foo/.", "bar"));
        Assertions.assertEquals("/foo/../bar", SyncUtil.combinedPath("/foo/..", "bar"));
        Assertions.assertEquals("/foo", SyncUtil.combinedPath("/foo", ""));
        Assertions.assertEquals("/foo", SyncUtil.combinedPath("/foo", " "));
        Assertions.assertEquals("/foo", SyncUtil.combinedPath("/foo", null));
        Assertions.assertEquals("/bar", SyncUtil.combinedPath(null, "/bar"));
        Assertions.assertEquals("/bar", SyncUtil.combinedPath(" ", "/bar"));
        Assertions.assertEquals("\\bar", SyncUtil.combinedPath("", "\\bar"));
    }

    @Test
    public void testParentPath() {
        Assertions.assertEquals("/foo", SyncUtil.parentPath("/foo/bar"));
        Assertions.assertEquals("/foo", SyncUtil.parentPath("/foo/bar/"));
        Assertions.assertEquals("/foo", SyncUtil.parentPath("/foo/."));
        Assertions.assertEquals("/foo", SyncUtil.parentPath("/foo/../"));
        Assertions.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\bar"));
        Assertions.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\bar\\"));
        Assertions.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\.\\"));
        Assertions.assertEquals("\\foo", SyncUtil.parentPath("\\foo\\.."));
        Assertions.assertEquals("/", SyncUtil.parentPath("/foo"));
        Assertions.assertEquals("/", SyncUtil.parentPath("/foo/"));
        Assertions.assertEquals("/", SyncUtil.parentPath("/."));
        Assertions.assertEquals("/", SyncUtil.parentPath("/../"));
        Assertions.assertEquals("\\", SyncUtil.parentPath("\\foo"));
        Assertions.assertEquals("\\", SyncUtil.parentPath("\\foo\\"));
        Assertions.assertEquals("\\", SyncUtil.parentPath("\\.\\"));
        Assertions.assertEquals("\\", SyncUtil.parentPath("\\.."));
        Assertions.assertNull(SyncUtil.parentPath("/"));
        Assertions.assertNull(SyncUtil.parentPath("\\"));
        Assertions.assertNull(SyncUtil.parentPath(""));
        Assertions.assertNull(SyncUtil.parentPath(null));
    }
}
