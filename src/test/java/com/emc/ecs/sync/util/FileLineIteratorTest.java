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

import com.emc.ecs.sync.test.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class FileLineIteratorTest {
    @Test
    public void testComments() throws IOException {
        File file = TestUtil.writeTempFile("alpha=bravo\n" + // standard
                "charlie=delta# comment here\n" + // regular comment
                "   # line should be skipped\n" + // comment line
                "\n" + // empty line
                "echo\\#one=foxtrot\n" + // escaped hash
                "  golf=hotel \t\n"); // trim test

        FileLineIterator i = new FileLineIterator(file.getPath());
        String line = i.next();
        Assert.assertEquals("alpha=bravo", line);
        line = i.next();
        Assert.assertEquals("charlie=delta", line);
        line = i.next();
        Assert.assertEquals("echo#one=foxtrot", line);
        line = i.next();
        Assert.assertEquals("golf=hotel", line);
        Assert.assertFalse(i.hasNext());
    }
}
