/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
package com.emc.atmos.sync.plugins.cas;

import com.emc.atmos.sync.util.CountingInputStream;
import com.filepool.fplibrary.FPStreamInterface;

import java.io.IOException;
import java.io.InputStream;

public class CasInputStream extends CountingInputStream implements FPStreamInterface {
    public static final int MAX_BUFFER = 1048576;

    private long size;

    public CasInputStream(InputStream in, long size) {
        super(in);
        this.size = size;
    }

    @Override
    public long getStreamLength() {
        return size;
    }

    @Override
    public boolean FPMarkSupported() {
        return super.markSupported();
    }

    @Override
    public void FPMark() {
        super.mark(MAX_BUFFER);
    }

    @Override
    public void FPReset() throws IOException {
        super.reset();
    }
}
