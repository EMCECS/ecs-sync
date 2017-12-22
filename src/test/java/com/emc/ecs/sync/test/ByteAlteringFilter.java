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

import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.DelegatingSyncObject;
import com.emc.ecs.sync.util.EnhancedInputStream;
import com.emc.ecs.sync.util.SyncUtil;

import javax.xml.bind.DatatypeConverter;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteAlteringFilter extends AbstractFilter<ByteAlteringFilter.ByteAlteringConfig> {
    private Random random = new Random();

    @Override
    public void filter(ObjectContext objectContext) {
        getNext().filter(objectContext);
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        SyncObject obj = getNext().reverseFilter(objectContext);
        if (obj.getMetadata().getContentLength() > 0) { // won't work on zero-byte objects
            if (random.nextBoolean()) {
                config.modifiedObjects.incrementAndGet();
                obj = new AlteredObject(obj);
            }
        }
        return obj;
    }

    @FilterConfig(cliName = "alter-data")
    public static class ByteAlteringConfig {
        AtomicInteger modifiedObjects = new AtomicInteger(0);

        public int getModifiedObjects() {
            return modifiedObjects.intValue();
        }
    }

    private class AlteredObject extends DelegatingSyncObject {
        private EnhancedInputStream in;
        private byte[] md5;

        AlteredObject(SyncObject delegate) {
            super(delegate);
        }

        @Override
        public synchronized InputStream getDataStream() {
            if (in == null) in = new EnhancedInputStream(new AlteredStream(delegate.getDataStream()), true);
            return in;
        }

        @Override
        public long getBytesRead() {
            if (in != null) {
                return in.getBytesRead();
            } else {
                return 0;
            }
        }

        synchronized byte[] getMd5(boolean forceRead) {
            if (md5 == null) {
                getDataStream();
                if (!in.isClosed()) {
                    if (!forceRead || in.getBytesRead() > 0)
                        throw new IllegalStateException("Cannot call getMd5 until stream is closed");
                    SyncUtil.consumeAndCloseStream(in);
                }
                md5 = in.getMd5Digest();
            }
            return md5;
        }

        @Override
        public String getMd5Hex(boolean forceRead) {
            return DatatypeConverter.printHexBinary(getMd5(forceRead));
        }
    }

    private class AlteredStream extends FilterInputStream {
        AlteredStream(InputStream inputStream) {
            super(inputStream);
        }

        @Override
        public int read() throws IOException {
            int result = super.read();
            if (result == -1) return result;
            return ((byte) result) + 1;
        }

        @Override
        public int read(byte[] bytes) throws IOException {
            return read(bytes, 0, bytes.length);
        }

        @Override
        public int read(byte[] bytes, int i, int i1) throws IOException {
            int result = super.read(bytes, i, i1);
            if (result > 0) bytes[i] += 1;
            return result;
        }
    }
}
