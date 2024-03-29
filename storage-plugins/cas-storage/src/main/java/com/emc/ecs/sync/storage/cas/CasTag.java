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
import com.filepool.fplibrary.FPTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public class CasTag extends FPTag implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(CasTag.class);

    private String clipId;
    private CloseListener listener;
    private volatile boolean closed = false;

    public CasTag(long l, FPClip fpClip, String clipId, CloseListener listener) {
        super(l, fpClip);
        this.clipId = clipId;
        this.listener = listener;
    }

    @Override
    public synchronized void close() {
        try {
            if (!closed) {
                Close();
                closed = true;
                fireTagClosed();
            }
        } catch (FPLibraryException e) {
            log.warn("could not close tag of clip " + clipId + ": " + CasStorage.summarizeError(e), e);
        } catch (Throwable t) {
            log.warn("could not close tag of clip " + clipId, t);
        }
    }

    private void fireTagClosed() {
        if (listener != null) listener.closed(clipId);
    }

    public long getTagRef() {
        return this.mTagRef;
    }

    public String getClipId() {
        return clipId;
    }

    public CloseListener getListener() {
        return listener;
    }
}
