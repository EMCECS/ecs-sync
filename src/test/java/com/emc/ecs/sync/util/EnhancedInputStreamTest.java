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
package com.emc.ecs.sync.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;

public class EnhancedInputStreamTest {
    @Test
    public void testCIS() throws Exception {
        int dataSize = 1038;
        byte[] buffer = new byte[dataSize];
        EnhancedInputStream cis = new EnhancedInputStream(new ByteArrayInputStream(buffer));
        cis.read();
        cis.read();
        SyncUtil.consumeAndCloseStream(cis);
        Assert.assertEquals(dataSize, cis.getBytesRead());
    }
}
