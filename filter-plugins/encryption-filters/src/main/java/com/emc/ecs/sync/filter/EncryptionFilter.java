/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.filter;

import com.emc.codec.CodecChain;
import com.emc.codec.encryption.EncryptionCodec;
import com.emc.codec.encryption.KeyProvider;
import com.emc.codec.encryption.KeystoreKeyProvider;
import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.EncryptionConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class EncryptionFilter extends AbstractFilter<EncryptionConfig> {
    private static final Logger log = LoggerFactory.getLogger(EncryptionFilter.class);

    private KeyProvider keyProvider;
    private CodecChain encodeChain;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (!options.isSyncMetadata())
            throw new ConfigurationException("EncryptionFilter requires syncing metadata");

        try {
            if (keyProvider == null) {
                if (config.getEncryptKeystore() == null) throw new ConfigurationException("Must specify a keystore");
                if (config.getEncryptKeyAlias() == null)
                    throw new ConfigurationException("Must specify the master key alias");

                // Init keystore
                KeyStore keystore = KeyStore.getInstance("jks");
                keystore.load(new FileInputStream(config.getEncryptKeystore()), config.getEncryptKeystorePass().toCharArray());
                log.info("Keystore Loaded");

                keyProvider = new KeystoreKeyProvider(keystore, config.getEncryptKeystorePass().toCharArray(),
                        config.getEncryptKeyAlias());
            }

            String cipherSpec = EncryptionCodec.AES_CBC_PKCS5_CIPHER;
            String encodeSpec = EncryptionCodec.encodeSpec(cipherSpec);
            encodeChain = new CodecChain(encodeSpec).withProperty(EncryptionCodec.PROP_KEY_PROVIDER, keyProvider);
            // check if we have an unlimited strength policy available
            if (Cipher.getMaxAllowedKeyLength(cipherSpec) >= 256) {
                log.info("using 256-bit cipher strength");
                encodeChain.addProperty(EncryptionCodec.PROP_KEY_SIZE, 256);
            } else {
                if (config.isEncryptForceStrong())
                    throw new ConfigurationException("strong encryption is not available");
                log.warn("strong encryption is unavailable; defaulting to 128-bit");
            }

        } catch (ConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Encryption is based on the Atmos Java SDK encryption standard (https://community.emc.com/docs/DOC-34465).
     */
    @Override
    public void filter(ObjectContext objectContext) {
        String sourceId = objectContext.getSourceSummary().getIdentifier();
        ObjectMetadata metadata = objectContext.getObject().getMetadata();
        if (metadata.isDirectory()) {
            // we can only encrypt data objects
            log.debug("skipping directory " + sourceId);
            getNext().filter(objectContext);
        } else {
            try {
                // see if the object is already encrypted via this method (encrypting again will overwrite the metadata
                // fields and make the object unreadable)
                String encodeSpec = metadata.getUserMetadataValue(CodecChain.META_TRANSFORM_MODE);
                if (encodeSpec != null) {
                    if (config.isFailIfEncrypted()) {
                        throw new UnsupportedOperationException("object is already encrypted (" + encodeSpec + ")");
                    } else {

                        // pass on untouched
                        log.info("object {} is already encrypted ({}); passing through untouched...",
                                sourceId, encodeSpec);
                        getNext().filter(objectContext);
                    }
                } else {
                    // get modifiable view of user metadata
                    Map<String, String> metaView = metadata.getUserMetadataValueMap();

                    InputStream dataStream = objectContext.getObject().getDataStream();

                    // change the object's data stream to be the encrypted stream
                    objectContext.getObject().setDataStream(encodeChain.getEncodeStream(dataStream, metaView));

                    // update size
                    if (encodeChain.isSizePredictable())
                        metadata.setContentLength(encodeChain.getEncodedSize(metadata.getContentLength()));

                    // remove any checksum from the metadata, as it will be invalid after decryption
                    metadata.setChecksum(null);

                    // update mtime if necessary
                    if (config.isEncryptUpdateMtime()) metadata.setModificationTime(new Date());

                    // metadata must be updated after data stream
                    objectContext.getObject().setPostStreamUpdateRequired(true);

                    // continue the filter chain
                    getNext().filter(objectContext);
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        SyncObject object = getNext().reverseFilter(objectContext);

        // only encrypting data objects
        if (object.getMetadata().isDirectory()) return object;

        // get modifiable view of user metadata
        Map<String, String> metaView = object.getMetadata().getUserMetadataValueMap();

        CodecChain decodeChain = new CodecChain(CodecChain.getEncodeSpecs(metaView))
                .withProperty(EncryptionCodec.PROP_KEY_PROVIDER, keyProvider);

        // change the object's data stream to be the encrypted stream
        object.setDataStream(decodeChain.getDecodeStream(object.getDataStream(), metaView));

        // update size
        if (decodeChain.isSizePredictable())
            object.getMetadata().setContentLength(decodeChain.getEncodedSize(object.getMetadata().getContentLength()));

        return object;
    }

    public KeyProvider getKeyProvider() {
        return keyProvider;
    }

    public void setKeyProvider(KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
    }
}
