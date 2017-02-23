/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage.file;

import com.emc.ecs.sync.config.storage.FilesystemConfig;

import java.io.*;

public class FilesystemStorage extends AbstractFilesystemStorage<FilesystemConfig> {
    protected InputStream createInputStream(File f) throws IOException {
        return new FileInputStream(f);
    }

    protected OutputStream createOutputStream(File f) throws IOException {
        return new FileOutputStream(f);
    }

    protected File createFile(String path) {
        return new File(path);
    }

    protected File createFile(File parent, String path) {
        return new File(parent, path);
    }
}
