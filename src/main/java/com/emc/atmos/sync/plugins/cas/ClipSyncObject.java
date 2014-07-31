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

import com.emc.atmos.sync.plugins.SyncObject;
import com.emc.atmos.sync.util.CountingInputStream;
import com.emc.atmos.sync.util.TimingUtil;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

public class ClipSyncObject extends SyncObject {
    private CasSource casSource;
    private String clipId;
    private FPClip clip;
    private long size;
    private CountingInputStream cin;
    private List<ClipTag> tags;

    public ClipSyncObject(CasSource casSource, String clipId, FPClip clip) throws URISyntaxException, FPLibraryException {
        this.casSource = casSource;
        this.clipId = clipId;
        this.clip = clip;
        size = clip.getTotalSize();
        tags = new LinkedList<ClipTag>();
        setSourceURI(CasUtil.generateSyncUri(casSource.getConnectionString(), clipId));
    }

    @Override
    public InputStream getInputStream() {
        if (cin == null) {
            synchronized (this) {
                if (cin == null) {
                    try {
                        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        TimingUtil.time(casSource, CasUtil.OPERATION_READ_CDF, new Callable<Void>() {
                            @Override
                            public Void call() throws Exception {
                                clip.RawRead(baos);
                                return null;
                            }
                        });

                        cin = new CountingInputStream(new ByteArrayInputStream(baos.toByteArray()));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return cin;
    }

    public long getBytesRead() {
        if (cin == null)
            return 0;
        return cin.getBytesRead() + aggregateBytesRead(tags);
    }

    protected long aggregateBytesRead(List<ClipTag> blobs) {
        long total = 0;
        for (ClipTag blob : blobs) {
            total += blob.getBytesRead();
        }
        return total;
    }

    @Override
    public String toString() {
        return "ClipSyncObject [" + clipId + "]";
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public String getRelativePath() {
        return CasUtil.generateRelativePath(clipId);
    }

    public String getClipId() {
        return clipId;
    }

    public FPClip getClip() {
        return clip;
    }

    public List<ClipTag> getTags() {
        return tags;
    }
}
