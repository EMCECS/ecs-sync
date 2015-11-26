/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.source;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;

public class SourceFilterTest {

    @Test
    public void testAccept() throws Exception {
        FilesystemSource source = new FilesystemSource();
        source.setExcludedFilenames(Arrays.asList("\\..*", ".*foo.*", ".*\\.bin"));
        source.setRootFile(new File("."));
        source.configure(null, null, null);

        FilenameFilter filter = source.getFilter();

        String[] positiveTests = new String[]{"bar.txt", "a.out", "this has spaces", "n.o.t.h.i.n.g"};
        for (String test : positiveTests) {
            Assert.assertTrue("filter should have accepted " + test, filter.accept(source.getRootFile(), test));
        }

        String[] negativeTests = new String[]{".svn", ".snapshots", ".f.o.o", "foo.txt", "ffoobar", "mywarez.bin",
                "in.the.round.bin"};
        for (String test : negativeTests) {
            Assert.assertFalse("filter should have rejected " + test, filter.accept(source.getRootFile(), test));
        }
    }
}