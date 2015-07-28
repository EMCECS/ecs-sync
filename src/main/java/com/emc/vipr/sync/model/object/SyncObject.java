/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.model.object;

import com.emc.vipr.sync.model.SyncMetadata;

import java.io.InputStream;

public interface SyncObject<I> {
    /**
     * Returns the source identifier in its raw form as implemented for the source system
     * (i.e. the Atmos ObjectIdentifier)
     */
    I getRawSourceIdentifier();

    /**
     * Returns the string representation of the full source identifier. Used in logging and reference updates.
     */
    String getSourceIdentifier();

    /**
     * Gets the relative path for the object.  If the target is a
     * namespace target, this path will be used when computing the
     * absolute path in the target, relative to the target root.
     */
    String getRelativePath();

    /**
     * Returns whether this object represents a directory or prefix. If false, assume this is a data object (even if
     * size is zero).
     */
    boolean isDirectory();

    /**
     * Returns the string representation of the full target identifier. Used in logging and reference updates.
     */
    String getTargetIdentifier();

    SyncMetadata getMetadata();

    /**
     * Specifies whether this object has stream-dependent metadata (metadata that changes after the object data is
     * streamed).  I.e. the encryption filter will add a checksum and unencrypted size.
     * <p/>
     * Targets must check this flag and, if set, update the object's metadata in the target after its data is
     * transferred.
     * <p/>
     * Default setting is false.  Override this method and return true if your object has metadata that changes after
     * its data is streamed.
     */
    boolean requiresPostStreamMetadataUpdate();

    void setTargetIdentifier(String targetIdentifier);

    void setMetadata(SyncMetadata metadata);

    InputStream getInputStream();

    long getBytesRead();

    String getMd5Hex(boolean forceRead);
}
