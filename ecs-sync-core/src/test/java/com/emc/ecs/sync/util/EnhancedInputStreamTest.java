/*
 * Copyright (c) 2015-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
        Assertions.assertEquals(dataSize, cis.getBytesRead());
    }
}
