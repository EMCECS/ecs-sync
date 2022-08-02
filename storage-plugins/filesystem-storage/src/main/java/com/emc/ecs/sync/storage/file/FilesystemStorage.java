/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage.file;

import com.emc.ecs.sync.config.storage.FilesystemConfig;

import java.io.*;

public class FilesystemStorage extends AbstractFilesystemStorage<FilesystemConfig> {
    @Override
    protected InputStream createInputStream(File f) throws IOException {
        return new FileInputStream(f);
    }

    @Override
    protected OutputStream createOutputStream(File f) throws IOException {
        return new FileOutputStream(f);
    }

    @Override
    public File createFile(String path) {
        return new File(path);
    }

    @Override
    public File createFile(File parent, String path) {
        return new File(parent, path);
    }
}
