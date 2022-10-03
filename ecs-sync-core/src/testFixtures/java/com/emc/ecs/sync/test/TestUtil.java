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
package com.emc.ecs.sync.test;

import com.emc.ecs.sync.EcsSync;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class TestUtil {
    public static File writeTempFile(String content) throws IOException {
        File tempFile = File.createTempFile("test", null);
        tempFile.deleteOnExit();

        OutputStream out = new FileOutputStream(tempFile);
        out.write(content.getBytes("UTF-8"));
        out.close();

        return tempFile;
    }

    /**
     * The theory behind this method is to contain all run/close logic in one place for unit tests - this is much
     * cleaner than having try-with-resources or run() then close() everywhere (there are about 100+ calls to
     * EcsSync.run() in unit tests)
     */
    public static void run(EcsSync sync) {
        sync.run();
        sync.close();
    }

    private TestUtil() {
    }
}
