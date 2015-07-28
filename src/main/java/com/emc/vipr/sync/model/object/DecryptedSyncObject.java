/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
import com.emc.vipr.sync.util.TransformUtil;
import com.emc.vipr.transform.InputTransform;
import com.emc.vipr.transform.TransformConstants;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Map;

public class DecryptedSyncObject extends AbstractSyncObject {
    private static final Logger l4j = Logger.getLogger(DecryptedSyncObject.class);

    private SyncObject delegate;
    private InputTransform transform;
    private boolean metadataComplete = false;

    @SuppressWarnings("unchecked")
    public DecryptedSyncObject(SyncObject delegate, InputTransform transform) {
        super(delegate.getRawSourceIdentifier(), delegate.getSourceIdentifier(),
                delegate.getRelativePath(), delegate.isDirectory());
        this.delegate = delegate;
        this.transform = transform;

        // set the decrypted size
        String decryptedSize = delegate.getMetadata().getUserMetadataValue(TransformConstants.META_ENCRYPTION_UNENC_SIZE);
        if (decryptedSize == null)
            throw new RuntimeException("encrypted object missing metadata field: " + TransformConstants.META_ENCRYPTION_UNENC_SIZE);

        delegate.getMetadata().setSize(Long.parseLong(decryptedSize));
    }

    @Override
    protected void loadObject() {
        // calling getMetadata() will call this method on delegate
    }

    @Override
    protected InputStream createSourceInputStream() {
        return transform.getDecodedInputStream();
    }

    @Override
    public Object getRawSourceIdentifier() {
        return delegate.getRawSourceIdentifier();
    }

    @Override
    public String getSourceIdentifier() {
        return delegate.getSourceIdentifier();
    }

    @Override
    public String getRelativePath() {
        return delegate.getRelativePath();
    }

    @Override
    public boolean isDirectory() {
        return delegate.isDirectory();
    }

    @Override
    public String getTargetIdentifier() {
        return delegate.getTargetIdentifier();
    }

    @Override
    public synchronized SyncMetadata getMetadata() {
        if (!metadataComplete) {
            try {
                Map<String, String> decryptedMetadata = transform.getDecodedMetadata();
                SyncMetadata objMetadata = delegate.getMetadata();

                // remove metadata keys if necessary
                for (String key : objMetadata.getUserMetadata().keySet()) {
                    if (!decryptedMetadata.containsKey(key)) objMetadata.getUserMetadata().remove(key);
                }

                // apply decrypted metadata
                for (String key : decryptedMetadata.keySet()) {
                    objMetadata.setUserMetadataValue(key, decryptedMetadata.get(key));
                }

                // TODO: remove when transforms remove their own metadata fields
                for (String key : TransformUtil.ENCRYPTION_METADATA_KEYS) {
                    objMetadata.getUserMetadata().remove(key);
                }

                // TODO: remove when transforms automatically modify the transform spec
                String transformSpec = objMetadata.getUserMetadataValue(TransformConstants.META_TRANSFORM_MODE);
                int pipeIndex = transformSpec.indexOf("|");
                if (pipeIndex > 0) {
                    objMetadata.setUserMetadataValue(TransformConstants.META_TRANSFORM_MODE,
                            transformSpec.substring(0, pipeIndex));
                } else {
                    objMetadata.getUserMetadata().remove(TransformConstants.META_TRANSFORM_MODE);
                }

                metadataComplete = true;
            } catch (IllegalStateException e) {
                l4j.debug("could not get decoded metadata - assuming object has not been streamed", e);
            }
        }
        return delegate.getMetadata();
    }

    @Override
    public boolean requiresPostStreamMetadataUpdate() {
        return true;
    }

    @Override
    public void setTargetIdentifier(String targetIdentifier) {
        delegate.setTargetIdentifier(targetIdentifier);
    }

    @Override
    public void setMetadata(SyncMetadata metadata) {
        delegate.setMetadata(metadata);
    }

    @Override
    public long getBytesRead() {
        return delegate.getBytesRead();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
