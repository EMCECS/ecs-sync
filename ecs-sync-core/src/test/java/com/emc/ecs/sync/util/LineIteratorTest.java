/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.test.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class LineIteratorTest {
    String[] lines = {
            /* 1 */ "alpha=bravo", // standard
            /* 2 */ "charlie=delta# comment here", // regular comment
            /* 3 */ "   # line should be skipped", // comment line
            /* 4 */ "", // empty line
            /* 5 */ "echo\\#one=foxtrot", // escaped hash
            /* 6 */ "  golf=hotel \t", // trim test
            /* 7 */ "\"foo,bar\"", // quoted comma
            /* 8 */ "\"foo#bar\" # baz", // quoted hash and unquoted hash
            /* 9 */ "foo,bar#, baz", // comma after hash
            /* 10 */ "\"foo#bar\"\"\" # baz", // quoted hash and unquoted hash plus an escaped quote
            /* 11 */ "\"foo \"\" bar\"", // basic escaped quote
            /* 12 */ "\"\"\"foo bar\"\"\"", // escaped quotes at the beginning and end of the line
            /* 13 */ "foo\\#bar\\#baz", // multiple escaped hashes
            /* 14 */ "foo,bar", // normal CSV line
            /* 15 */ "\"foo\\#bar\",\"biz\\#baz\"" // escaped hashes inside quotes
    };
    String fileStr = Arrays.stream(lines).map(line -> line + "\n").collect(Collectors.joining());

    @Test
    public void testComments() throws IOException {
        File file = TestUtil.writeTempFile(fileStr);

        LineIterator i = new LineIterator(file.getPath());
        String line = i.next(); // line 1
        Assertions.assertEquals("alpha=bravo", line);
        line = i.next(); // line 2
        Assertions.assertEquals("charlie=delta", line);
        line = i.next(); // line 5
        Assertions.assertEquals("echo#one=foxtrot", line);
        line = i.next(); // line 6
        Assertions.assertEquals("golf=hotel", line);
        line = i.next(); // line 7
        Assertions.assertEquals("\"foo,bar\"", line);
        line = i.next(); // line 8
        Assertions.assertEquals("\"foo#bar\"", line);
        line = i.next(); // line 9
        Assertions.assertEquals("foo,bar", line);
        line = i.next(); // line 10
        Assertions.assertEquals("\"foo#bar\"\"\"", line);
        line = i.next(); // line 11
        Assertions.assertEquals("\"foo \"\" bar\"", line);
        line = i.next(); // line 12
        Assertions.assertEquals("\"\"\"foo bar\"\"\"", line);
        line = i.next(); // line 13
        Assertions.assertEquals("foo#bar#baz", line);
        line = i.next(); // line 14
        Assertions.assertEquals("foo,bar", line);
        line = i.next(); // line 15
        Assertions.assertEquals("\"foo\\#bar\",\"biz\\#baz\"", line);
        Assertions.assertFalse(i.hasNext());
    }

    @Test
    public void testRawValues() throws Exception {
        File file = TestUtil.writeTempFile(fileStr);

        LineIterator i = new LineIterator(file.getPath(), true);
        String line = i.next();
        Assertions.assertEquals(lines[0], line);
        line = i.next();
        Assertions.assertEquals(lines[1], line);
        line = i.next();
        Assertions.assertEquals(lines[2], line);
        line = i.next(); // line 4 is the only raw blank line that should be skipped
        Assertions.assertEquals(lines[4], line);
        line = i.next();
        Assertions.assertEquals(lines[5], line);
        line = i.next();
        Assertions.assertEquals(lines[6], line);
        line = i.next();
        Assertions.assertEquals(lines[7], line);
        line = i.next();
        Assertions.assertEquals(lines[8], line);
        line = i.next();
        Assertions.assertEquals(lines[9], line);
        line = i.next();
        Assertions.assertEquals(lines[10], line);
        line = i.next();
        Assertions.assertEquals(lines[11], line);
        line = i.next();
        Assertions.assertEquals(lines[12], line);
        line = i.next();
        Assertions.assertEquals(lines[13], line);
        line = i.next();
        Assertions.assertEquals(lines[14], line);
        Assertions.assertFalse(i.hasNext());
    }

    @Test
    public void testCasCsv() throws IOException {
        String[] lines = {
                "ASTKGAB4EJO8De514VG3K3EMLG0G41F9JREVKO09B8P27M622A5EM,test-1",
                "23MO0DIP9G2E5eBID75KAJLOJBKG41F9JRF0MK0NB2DT1BA2HO19J,test-2",
                "052HDK8CAC6H5e7BSVG2MHF3E2LG41F9JRF1ML0P3GP41RAPJOJ12,test-3",
                "0DGUKV1O0MOB1e0UNR7CLEVA76VG41F9JRF2MM0L4ATF2R5CK9LE4,test-4"
        };
        String fileStr = Arrays.stream(lines).map(line -> line + "\n").collect(Collectors.joining());

        File file = TestUtil.writeTempFile(fileStr);

        LineIterator i = new LineIterator(file.getPath());
        String line = i.next();
        Assertions.assertEquals(lines[0], line);
        line = i.next();
        Assertions.assertEquals(lines[1], line);
        line = i.next();
        Assertions.assertEquals(lines[2], line);
        line = i.next();
        Assertions.assertEquals(lines[3], line);
        Assertions.assertFalse(i.hasNext());
    }
}
