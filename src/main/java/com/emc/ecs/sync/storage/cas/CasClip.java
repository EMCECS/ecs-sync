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
package com.emc.ecs.sync.storage.cas;

import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryException;
import com.filepool.natives.FPLibraryNative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class CasClip extends FPClip implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(CasClip.class);

    private CasPool casPool;
    private String clipId;
    private volatile boolean closed = false;

    public CasClip(CasPool fpPool, String s, int i) throws FPLibraryException {
        super(fpPool, s, i);
        this.casPool = fpPool;
        this.clipId = s;
    }

    public CasClip(CasPool fpPool, String s) throws FPLibraryException {
        super(fpPool, s);
        this.casPool = fpPool;
        this.clipId = s;
    }

    public CasClip(CasPool fpPool) throws FPLibraryException {
        super(fpPool);
        this.casPool = fpPool;
    }

    public CasClip(CasPool fpPool, String s, InputStream inputStream, long l) throws FPLibraryException, IOException {
        super(fpPool, s, inputStream, l);
        this.casPool = fpPool;
        this.clipId = s;
    }

    @Override
    public CasTag FetchNext() throws FPLibraryException {
        if (this.mPool != null && this.casPool.getPoolRef() == 0L) {
            throw new FPLibraryException(-10039);
        } else if (this.mClipRef == 0L) {
            throw new FPLibraryException(-10038);
        } else {
            long var1 = FPLibraryNative.FPClip_FetchNext(this.mClipRef);
            if (FPLibraryNative.getLastError() != 0) {
                throw new FPLibraryException();
            } else {
                return var1 != 0L ? new CasTag(var1, this, clipId) : null;
            }
        }
    }

    @Override
    protected void finalize() {
        close();
        super.finalize();
    }

    @Override
    public synchronized void close() {
        try {
            if (!closed) {
                Close();
                closed = true;
            }
        } catch (FPLibraryException e) {
            log.warn("could not close clip " + clipId + ": " + CasStorage.summarizeError(e), e);
        } catch (Throwable t) {
            log.warn("could not close clip " + clipId, t);
        }
    }

    public long getClipRef() {
        return this.mClipRef;
    }
}
