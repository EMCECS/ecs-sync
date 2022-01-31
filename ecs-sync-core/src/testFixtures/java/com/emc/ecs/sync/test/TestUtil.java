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
package com.emc.ecs.sync.test;

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

    private TestUtil() {
    }
}
