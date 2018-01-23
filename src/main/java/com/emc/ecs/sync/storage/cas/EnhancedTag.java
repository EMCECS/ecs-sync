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

import com.emc.ecs.sync.util.SyncUtil;
import com.emc.object.util.ProgressListener;
import com.filepool.fplibrary.FPLibraryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class EnhancedTag implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EnhancedTag.class);

    private CasTag tag;
    private int tagNum;
    private int bufferSize;
    private boolean drainOnError;
    private boolean blobAttached = false;
    private ProgressListener listener;
    private ExecutorService readExecutor;
    private BlobInputStream blobInputStream;

    public EnhancedTag(CasTag tag, int tagNum, int bufferSize, ProgressListener listener) throws FPLibraryException {
        this(tag, tagNum, bufferSize, listener, null);
    }

    public EnhancedTag(CasTag tag, int tagNum, int bufferSize, ProgressListener listener, ExecutorService readExecutor) throws FPLibraryException {
        this.tag = tag;
        this.tagNum = tagNum;
        this.bufferSize = bufferSize;
        this.listener = listener;
        this.readExecutor = readExecutor;

        int blobStatus = tag.BlobExists();
        if (blobStatus == 1) {
            blobAttached = true;
        } else {
            // no data attached to this tag
            if (blobStatus != -1)
                throw new RuntimeException("[" + tag.getClipId() + "." + tagNum + "]: blob unavailable (status=" + blobStatus + ")");
        }
    }

    public synchronized BlobInputStream getBlobInputStream() throws IOException {
        if (blobInputStream == null) {
            if (!blobAttached) throw new UnsupportedOperationException("this tag has no blob data");
            if (tag == null) throw new UnsupportedOperationException("this tag has been closed");
            blobInputStream = new BlobInputStream(tag, bufferSize, listener, readExecutor);
            blobInputStream.setDrainOnError(drainOnError);
        }
        return blobInputStream;
    }

    public long getBytesRead() {
        if (blobInputStream == null) return 0;
        return blobInputStream.getBytesRead();
    }

    public synchronized byte[] getMd5Digest(boolean forceRead) {
        if (!forceRead && blobInputStream == null) return null;
        try {
            getBlobInputStream(); // make sure blob stream is loaded
            if (!blobInputStream.isClosed()) {
                if (!forceRead || blobInputStream.getBytesRead() > 0)
                    throw new IllegalStateException("getMd5Digest() called before tag was fully streamed");
                SyncUtil.consumeAndCloseStream(blobInputStream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return blobInputStream.getMd5Digest();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }

    @Override
    public void close() {
        try {
            if (tag != null) tag.close();
        } catch (Throwable t) {
            log.warn("could not close tag of clip " + tag.getClipId(), t);
        } finally {
            tag = null; // remove reference to free memory
        }
    }

    public CasTag getTag() {
        return tag;
    }

    public int getTagNum() {
        return tagNum;
    }

    public boolean isBlobAttached() {
        return blobAttached;
    }

    public boolean isDrainOnError() {
        return drainOnError;
    }

    public void setDrainOnError(boolean drainOnError) {
        this.drainOnError = drainOnError;
    }
}
