/*
 * Copyright (c) 2014-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.ArchiveConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.test.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class ArchiveStorageTest {
    private static final byte[] TAR_GZ_CONTENT = {
            (byte) 0x1f, (byte) 0x8b, (byte) 0x08, (byte) 0x08, (byte) 0x06, (byte) 0xc1, (byte) 0x6b, (byte) 0x53, (byte) 0x00, (byte) 0x03, (byte) 0x74, (byte) 0x65, (byte) 0x73, (byte) 0x74, (byte) 0x2e, (byte) 0x74,
            (byte) 0x61, (byte) 0x72, (byte) 0x00, (byte) 0xed, (byte) 0x97, (byte) 0xcf, (byte) 0x4a, (byte) 0xc3, (byte) 0x40, (byte) 0x10, (byte) 0xc6, (byte) 0x73, (byte) 0x0e, (byte) 0xf8, (byte) 0x0e, (byte) 0x7b,
            (byte) 0xf6, (byte) 0xd0, (byte) 0xce, (byte) 0xce, (byte) 0xfe, (byte) 0x6b, (byte) 0x0e, (byte) 0x39, (byte) 0xa8, (byte) 0x08, (byte) 0x2d, (byte) 0xf4, (byte) 0x50, (byte) 0x54, (byte) 0x04, (byte) 0x8f,
            (byte) 0xa1, (byte) 0x59, (byte) 0x4a, (byte) 0x50, (byte) 0x53, (byte) 0x48, (byte) 0x53, (byte) 0xe9, (byte) 0xb1, (byte) 0x6f, (byte) 0xe3, (byte) 0x45, (byte) 0x7c, (byte) 0x04, (byte) 0xdf, (byte) 0xca,
            (byte) 0xbb, (byte) 0x9b, (byte) 0xd8, (byte) 0x34, (byte) 0x34, (byte) 0x55, (byte) 0xb1, (byte) 0x25, (byte) 0x49, (byte) 0x95, (byte) 0xee, (byte) 0xef, (byte) 0xb2, (byte) 0x61, (byte) 0x58, (byte) 0xb2,
            (byte) 0x93, (byte) 0x0c, (byte) 0xdf, (byte) 0xce, (byte) 0x7c, (byte) 0xa3, (byte) 0x60, (byte) 0xd1, (byte) 0xd7, (byte) 0x41, (byte) 0xa8, (byte) 0x93, (byte) 0x6e, (byte) 0xaa, (byte) 0x67, (byte) 0x69,
            (byte) 0x27, (byte) 0x5d, (byte) 0xa4, (byte) 0x4e, (byte) 0xed, (byte) 0x00, (byte) 0x80, (byte) 0xe4, (byte) 0x9c, (byte) 0x38, (byte) 0x2a, (byte) 0xa7, (byte) 0x5c, (byte) 0x61, (byte) 0x05, (byte) 0x52,
            (byte) 0x24, (byte) 0x14, (byte) 0x19, (byte) 0x43, (byte) 0xc5, (byte) 0x81, (byte) 0x71, (byte) 0x45, (byte) 0x80, (byte) 0x72, (byte) 0x64, (byte) 0xd2, (byte) 0x21, (byte) 0x8b, (byte) 0xfa, (byte) 0x53,
            (byte) 0xd9, (byte) 0x66, (byte) 0x3e, (byte) 0x4b, (byte) 0x83, (byte) 0xc4, (byte) 0xa4, (byte) 0x12, (byte) 0x24, (byte) 0xb1, (byte) 0x4e, (byte) 0xc7, (byte) 0xfb, (byte) 0xbf, (byte) 0xe7, (byte) 0xf3,
            (byte) 0x53, (byte) 0xc8, (byte) 0x7a, (byte) 0xfd, (byte) 0x27, (byte) 0x50, (byte) 0x45, (byte) 0x26, (byte) 0x51, (byte) 0xe8, (byte) 0x2b, (byte) 0x04, (byte) 0xc5, (byte) 0x7b, (byte) 0x08, (byte) 0xd2,
            (byte) 0xa5, (byte) 0x3d, (byte) 0x32, (byte) 0x37, (byte) 0x01, (byte) 0xaa, (byte) 0x40, (byte) 0x28, (byte) 0xc5, (byte) 0x05, (byte) 0x30, (byte) 0x17, (byte) 0x81, (byte) 0x8c, (byte) 0xd3, (byte) 0xe8,
            (byte) 0x51, (byte) 0xfb, (byte) 0x94, (byte) 0x79, (byte) 0x9e, (byte) 0x50, (byte) 0x20, (byte) 0x65, (byte) 0x1e, (byte) 0x0b, (byte) 0x36, (byte) 0x63, (byte) 0xdc, (byte) 0x45, (byte) 0x46, (byte) 0xae,
            (byte) 0x2f, (byte) 0xfa, (byte) 0x83, (byte) 0xe1, (byte) 0x5d, (byte) 0x27, (byte) 0xd4, (byte) 0x4f, (byte) 0x3e, (byte) 0x95, (byte) 0xa6, (byte) 0xc6, (byte) 0x88, (byte) 0xe0, (byte) 0x22, (byte) 0x16,
            (byte) 0xe1, (byte) 0x28, (byte) 0x9e, (byte) 0xfa, (byte) 0xcc, (byte) 0x03, (byte) 0x8f, (byte) 0x22, (byte) 0x66, (byte) 0xc7, (byte) 0xac, (byte) 0xa2, (byte) 0xf1, (byte) 0x43, (byte) 0x14, (byte) 0xdf,
            (byte) 0xfb, (byte) 0xd4, (byte) 0x3d, (byte) 0xf4, (byte) 0x9f, (byte) 0x38, (byte) 0x4e, (byte) 0x9a, (byte) 0x53, (byte) 0x7d, (byte) 0x49, (byte) 0xa1, (byte) 0xff, (byte) 0xa5, (byte) 0x79, (byte) 0xd6,
            (byte) 0x2f, (byte) 0x27, (byte) 0xcf, (byte) 0xd9, (byte) 0x7a, (byte) 0xfa, (byte) 0xfe, (byte) 0xf6, (byte) 0x5a, (byte) 0xe8, (byte) 0x1f, (byte) 0xa8, (byte) 0xa8, (byte) 0xe8, (byte) 0x9f, (byte) 0x71,
            (byte) 0x89, (byte) 0x0e, (byte) 0x81, (byte) 0x26, (byte) 0x93, (byte) 0x2a, (byte) 0x38, (byte) 0x72, (byte) 0xfd, (byte) 0xdf, (byte) 0x98, (byte) 0xfa, (byte) 0x47, (byte) 0xf1, (byte) 0x84, (byte) 0x50,
            (byte) 0x82, (byte) 0x84, (byte) 0x1d, (byte) 0x3a, (byte) 0x19, (byte) 0x4b, (byte) 0xeb, (byte) 0x8c, (byte) 0xd6, (byte) 0xfd, (byte) 0x3f, (byte) 0x8c, (byte) 0x92, (byte) 0x86, (byte) 0xce, (byte) 0x30,
            (byte) 0x7a, (byte) 0x50, (byte) 0x42, (byte) 0xfc, (byte) 0xd0, (byte) 0xff, (byte) 0x25, (byte) 0x94, (byte) 0xfa, (byte) 0x97, (byte) 0xb9, (byte) 0xfe, (byte) 0x11, (byte) 0x95, (byte) 0xed, (byte) 0xff,
            (byte) 0x6d, (byte) 0xb0, (byte) 0x4f, (byte) 0xff, (byte) 0x57, (byte) 0xde, (byte) 0x17, (byte) 0xfd, (byte) 0xdf, (byte) 0x73, (byte) 0x59, (byte) 0x8f, (byte) 0x0c, (byte) 0x07, (byte) 0xe7, (byte) 0x67,
            (byte) 0x57, (byte) 0xa6, (byte) 0xad, (byte) 0xdf, (byte) 0x5e, (byte) 0x76, (byte) 0xc6, (byte) 0x89, (byte) 0x36, (byte) 0x3b, (byte) 0xa6, (byte) 0x71, (byte) 0x75, (byte) 0xd7, (byte) 0xef, (byte) 0xa7,
            (byte) 0x04, (byte) 0x06, (byte) 0x5b, (byte) 0x53, (byte) 0x02, (byte) 0xb3, (byte) 0x53, (byte) 0x42, (byte) 0xcd, (byte) 0x18, (byte) 0xd5, (byte) 0x77, (byte) 0x9b, (byte) 0x3e, (byte) 0xa3, (byte) 0xd0,
            (byte) 0xff, (byte) 0xd2, (byte) 0xf9, (byte) 0xa6, (byte) 0xff, (byte) 0x43, (byte) 0x55, (byte) 0xff, (byte) 0x28, (byte) 0xd0, (byte) 0xcc, (byte) 0xff, (byte) 0xa2, (byte) 0xe9, (byte) 0xc4, (byte) 0x32,
            (byte) 0x8e, (byte) 0x5c, (byte) 0xff, (byte) 0x59, (byte) 0xfd, (byte) 0x47, (byte) 0x1b, (byte) 0x1e, (byte) 0x10, (byte) 0x6b, (byte) 0x1f, (byte) 0x07, (byte) 0x77, (byte) 0xf2, (byte) 0x7f, (byte) 0x79,
            (byte) 0xfd, (byte) 0x05, (byte) 0x05, (byte) 0xb0, (byte) 0xf7, (byte) 0x7f, (byte) 0x1b, (byte) 0xd4, (byte) 0x75, (byte) 0xff, (byte) 0xab, (byte) 0x9d, (byte) 0x6e, (byte) 0x76, (byte) 0xeb, (byte) 0xff,
            (byte) 0xfe, (byte) 0x0a, (byte) 0x99, (byte) 0xfe, (byte) 0x9b, (byte) 0x51, (byte) 0x7d, (byte) 0xc9, (byte) 0x4e, (byte) 0xfe, (byte) 0x2f, (byte) 0xd7, (byte) 0x3f, (byte) 0x67, (byte) 0xc8, (byte) 0xad,
            (byte) 0xff, (byte) 0x6b, (byte) 0x83, (byte) 0xc2, (byte) 0xff, (byte) 0x31, (byte) 0xe3, (byte) 0xff, (byte) 0xe8, (byte) 0xa1, (byte) 0x93, (byte) 0xb1, (byte) 0x58, (byte) 0x2c, (byte) 0x16, (byte) 0x4b,
            (byte) 0x6b, (byte) 0x7c, (byte) 0x00, (byte) 0xac, (byte) 0x15, (byte) 0x1d, (byte) 0x8e, (byte) 0x00, (byte) 0x1a, (byte) 0x00, (byte) 0x00
    };

    @Test
    public void testTarGzip() throws Exception {
        File file = File.createTempFile("test", ".tar.gz");
        file.deleteOnExit();
        writeFile(file, TAR_GZ_CONTENT);

        ArchiveConfig archiveConfig = new ArchiveConfig();
        archiveConfig.setPath(file.getPath());

        TestConfig testConfig = new TestConfig().withReadData(true).withDiscardData(false);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(new SyncConfig().withSource(archiveConfig).withTarget(testConfig));
        TestUtil.run(sync);

        Assertions.assertEquals(0, sync.getStats().getObjectsFailed(), "Failures detected");
        Assertions.assertEquals(3, sync.getStats().getObjectsComplete(), "Wrong number of files transferred");
        Assertions.assertEquals(26, sync.getStats().getBytesComplete(), "Wrong number of bytes transferred");
    }

    private void writeFile(File file, byte[] content) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        out.write(content);
        out.close();
    }
}
