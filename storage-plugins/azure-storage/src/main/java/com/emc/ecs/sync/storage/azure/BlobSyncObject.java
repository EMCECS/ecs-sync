/*
 * Copyright (c) 2020-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage.azure;

import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.EnhancedInputStream;
import com.emc.ecs.sync.util.SyncUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class BlobSyncObject extends SyncObject {
    private static final Logger log = LoggerFactory.getLogger(BlobSyncObject.class);

    private String snapshotId;

    BlobSyncObject(SyncStorage source, String relativePath, ObjectMetadata metadata) {
        super(source, relativePath, metadata);
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    void setSnapshotId(String snapshotId) {
        this.snapshotId = snapshotId;
    }

    @Override
    public String getMd5Hex(boolean forceRead) {
        List snapshots = (List) getProperty(AzureBlobStorage.PROP_BLOB_SNAPSHOTS);
        if (snapshots == null) {
            throw new RuntimeException("could not get snapshots when get md5");
        }

        StringBuilder canonicalString = new StringBuilder("[");
        for (Object snapshot : snapshots) {
            BlobSyncObject syncObject = (BlobSyncObject) snapshot;
            String md5 = getMd5(forceRead, (EnhancedInputStream) syncObject.getDataStream());
            canonicalString.append("{")
                    .append("\"deleteMarker\":").append(false)
                    .append("\"md5\":\"").append(md5).append("\"")
                    .append("}");
        }
        canonicalString.append("]");
        try {
            MessageDigest digest = MessageDigest.getInstance("md5");
            return DatatypeConverter.printHexBinary(digest.digest(canonicalString.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not initialize MD5", e);
        }
    }

    private synchronized String getMd5(boolean forceRead, EnhancedInputStream enhancedStream) {
        if (!enhancedStream.isClosed()) {
            if (!forceRead || enhancedStream.getBytesRead() > 0) {
                throw new IllegalStateException("Cannot call getMd5 until stream is closed");
            }
            SyncUtil.consumeAndCloseStream(enhancedStream);
        }
        byte[] md5 = enhancedStream.getMd5Digest();
        return DatatypeConverter.printHexBinary(md5);
    }
}
