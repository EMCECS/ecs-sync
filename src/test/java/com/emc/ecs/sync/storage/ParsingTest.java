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
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import org.junit.Assert;
import org.junit.Test;

public class ParsingTest {
    @Test
    public void testFilesystemUriParsing() {
        FilesystemConfig fsConfig = new FilesystemConfig();

        String uri = "file:///foo/bar", path = "/foo/bar", newUri = "file:/foo/bar";
        ConfigUtil.parseUri(fsConfig, uri);
        Assert.assertEquals(path, fsConfig.getPath());
        Assert.assertEquals(newUri, ConfigUtil.generateUri(fsConfig));

        uri = "file://foo/bar";
        path = "foo/bar";
        newUri = "file:foo/bar";
        ConfigUtil.parseUri(fsConfig, uri);
        Assert.assertEquals(path, fsConfig.getPath());
        Assert.assertEquals(newUri, ConfigUtil.generateUri(fsConfig));
    }
}
