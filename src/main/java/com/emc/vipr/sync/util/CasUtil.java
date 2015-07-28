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
package com.emc.vipr.sync.util;

import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.object.ClipSyncObject;
import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPLibraryConstants;
import com.filepool.fplibrary.FPPool;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;

public class CasUtil {
    /**
     * This pattern is used to activate the CAS plugins.
     */
    public static final String URI_PATTERN = "^cas://([^/]*?)(:[0-9]+)?(,([^/]*?)(:[0-9]+)?)*\\?.*$";
    public static final String URI_PREFIX = "cas://";

    public static final String OPERATION_FETCH_QUERY_RESULT = "CasFetchQueryResult";
    public static final String OPERATION_OPEN_CLIP = "CasOpenClip";
    public static final String OPERATION_READ_CDF = "CasReadCdf";
    public static final String OPERATION_WRITE_CDF = "CasWriteCdf";
    public static final String OPERATION_STREAM_BLOB = "CasStreamBlob";
    public static final String OPERATION_WRITE_CLIP = "CasWriteClip";
    public static final String OPERATION_TOTAL = "TotalTime";

    private CasUtil() {
    }

    public static FPClip openClip(SyncPlugin plugin, final FPPool pool, final String clipId) throws Exception {
        return TimingUtil.time(plugin, CasUtil.OPERATION_OPEN_CLIP, new Callable<FPClip>() {
            @Override
            public FPClip call() throws Exception {
                return new FPClip(pool, clipId, FPLibraryConstants.FP_OPEN_FLAT);
            }
        });
    }

    public static void hydrateClipData(SyncPlugin plugin, ClipSyncObject syncObject, final FPClip clip) throws Exception {
        // pull the CDF
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TimingUtil.time(plugin, CasUtil.OPERATION_READ_CDF, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                clip.RawRead(baos);
                return null;
            }
        });
        syncObject.setClipName(clip.getName());
        syncObject.setCdfData(baos.toByteArray());

        SyncMetadata metadata = new SyncMetadata();
        metadata.setSize(clip.getTotalSize());
        syncObject.setMetadata(metadata);
    }

    public static URI generateSyncUri(String connectionString, String clipId) throws URISyntaxException {
        return new URI("cas://" + connectionString + "/" + clipId);
    }

    public static String generateRelativePath(String clipId) {
        return clipId + ".cdf";
    }
}
