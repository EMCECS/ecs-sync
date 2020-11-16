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

public class LineIteratorTest {
    @Test
    public void testComments() throws IOException {
        File file = TestUtil.writeTempFile("alpha=bravo\n" + // standard
                "charlie=delta# comment here\n" + // regular comment
                "   # line should be skipped\n" + // comment line
                "\n" + // empty line
                "echo\\#one=foxtrot\n" + // escaped hash
                "  golf=hotel \t\n"); // trim test

        LineIterator i = new LineIterator(file.getPath());
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

    @Test
    public void testRawValues() throws Exception {
        String line1 = "alpha=bravo";// standard
        String line2 = "charlie=delta# comment here";// regular comment
        String line3 = "   # line should be skipped";// comment line
        String line4 = "";// empty line
        String line5 = "echo\\#one=foxtrot";// escaped hash
        String line6 = "  golf=hotel \t";// trim test
        File file = TestUtil.writeTempFile(
                line1 + "\n" +
                        line2 + "\n" +
                        line3 + "\n" +
                        line4 + "\n" +
                        line5 + "\n" +
                        line6 + "\n"
        );

        LineIterator i = new LineIterator(file.getPath(), true);
        String line = i.next();
        Assert.assertEquals(line1, line);
        line = i.next();
        Assert.assertEquals(line2, line);
        line = i.next();
        Assert.assertEquals(line3, line);
        line = i.next(); // line4 is the only raw blank line that should be skipped
        Assert.assertEquals(line5, line);
        line = i.next();
        Assert.assertEquals(line6, line);
        Assert.assertFalse(i.hasNext());
    }
}
