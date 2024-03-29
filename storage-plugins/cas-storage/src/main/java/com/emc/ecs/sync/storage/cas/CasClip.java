/*
 * Copyright (c) 2017-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage.cas;

import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryException;
import com.filepool.natives.FPLibraryNative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class CasClip extends FPClip implements Closeable, CloseListener {
    private static final Logger log = LoggerFactory.getLogger(CasClip.class);

    private final CasPool casPool;
    private String clipId;
    private volatile boolean closed = false;
    private CloseListener listener;
    private boolean synchronizeClipClose;
    private AtomicInteger openTagCount = new AtomicInteger();

    public CasClip(CasPool fpPool, String s, int i) throws FPLibraryException {
        super(fpPool, s, i);
        this.casPool = fpPool;
        this.clipId = s;
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
                CasTag casTag = var1 != 0L ? new CasTag(var1, this, clipId, this) : null;
                if (casTag != null) openTagCount.incrementAndGet();
                return casTag;
            }
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (!closed) {
                log.debug("closing CA {} with {} open tags", clipId, getOpenTagCount());
                if (synchronizeClipClose) synchronized (casPool) {
                    Close();
                }
                else Close();
                closed = true;
                log.debug("closed CA {} successfully", clipId);
                fireClipClosed();
            }
        } catch (FPLibraryException e) {
            log.warn("could not close clip " + clipId + ": " + CasStorage.summarizeError(e), e);
        } catch (Throwable t) {
            log.warn("could not close clip " + clipId, t);
        }
    }

    @Override
    public void closed(String identifier) {
        openTagCount.decrementAndGet();
    }

    private void fireClipClosed() {
        if (listener != null) listener.closed(clipId);
    }

    public long getClipRef() {
        return this.mClipRef;
    }

    public int getOpenTagCount() {
        return openTagCount.get();
    }

    public CloseListener getListener() {
        return listener;
    }

    public void setListener(CloseListener listener) {
        this.listener = listener;
    }

    public boolean isSynchronizeClipClose() {
        return synchronizeClipClose;
    }

    public void setSynchronizeClipClose(boolean synchronizeClipClose) {
        this.synchronizeClipClose = synchronizeClipClose;
    }

    public CasClip withListener(CloseListener listener) {
        setListener(listener);
        return this;
    }

    public CasClip withSynchronizeClose(boolean synchronizeClose) {
        setSynchronizeClipClose(synchronizeClose);
        return this;
    }
}
