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

import com.emc.atmos.sync.util.TimingUtil;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryConstants;
import com.filepool.fplibrary.FPPool;
import com.filepool.fplibrary.FPTag;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class ReadClipTask implements Runnable {
    private static final Logger l4j = Logger.getLogger(ReadClipTask.class);

    private CasSource casSource;
    private FPPool pool;
    private String clipId;
    private int bufferSize;

    public ReadClipTask(CasSource casSource, String clipId, int bufferSize) {
        this.casSource = casSource;
        this.pool = casSource.pool;
        this.clipId = clipId;
        this.bufferSize = bufferSize;
    }

    @Override
    public void run() {
        ClipSyncObject clipSync = null;
        FPClip clip = null;
        FPTag tag = null;
        int tagCount = 0;
        try {
            // the entire clip (and all blobs) will be sent at once, so we can keep references to clips and tags open.
            // open the clip
            clip = TimingUtil.time(casSource, CasUtil.OPERATION_OPEN_CLIP, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    return new FPClip(pool, clipId, FPLibraryConstants.FP_OPEN_FLAT);
                }
            });

            clipSync = new ClipSyncObject(casSource, clipId, clip);

            // pull all clip tags
            while ((tag = clip.FetchNext()) != null) {
                clipSync.getTags().add(new ClipTag(tag, tagCount++, bufferSize));
            }

            casSource.getNext().filter(clipSync);

            casSource.complete(clipSync);
        } catch (Throwable t) {
            if (clipSync == null) l4j.warn("could not create sync object for " + clipId, t);
            else casSource.failed(clipSync, t);
        } finally {
            // close current tag ref
            try {
                if (tag != null) tag.Close();
            } catch (Throwable t) {
                l4j.warn("could not close tag " + clipId + "." + tagCount, t);
            }
            // close blob tags
            if (clipSync != null) {
                for (ClipTag blobSync : clipSync.getTags()) {
                    try {
                        blobSync.getTag().Close();
                    } catch (Throwable t) {
                        l4j.warn("could not close tag " + clipId + "." + blobSync.getTagNum(), t);
                    }
                }
            }
            // close clip
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                l4j.warn("could not close clip " + clipId, t);
            }
        }
    }

    @Override
    public String toString() {
        return "ReadClipTask [" + clipId + "]";
    }
}
