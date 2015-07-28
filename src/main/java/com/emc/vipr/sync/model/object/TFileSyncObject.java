/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.model.object;

import com.emc.vipr.sync.SyncPlugin;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileInputStream;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class TFileSyncObject extends FileSyncObject {
    public TFileSyncObject(SyncPlugin parentPlugin, MimetypesFileTypeMap mimeMap, File sourceFile, String relativePath) {
        super(parentPlugin, mimeMap, sourceFile, relativePath);
    }

    @Override
    protected InputStream createInputStream(File f) throws IOException {
        return new TFileInputStream(f);
    }

    @Override
    protected File createFile(String path) {
        return new TFile(path);
    }
}
