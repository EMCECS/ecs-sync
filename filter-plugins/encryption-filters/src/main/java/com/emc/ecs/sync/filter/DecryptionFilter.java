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
package com.emc.ecs.sync.filter;

import com.emc.codec.CodecChain;
import com.emc.codec.encryption.EncryptionCodec;
import com.emc.codec.encryption.EncryptionConstants;
import com.emc.codec.encryption.KeyProvider;
import com.emc.codec.encryption.KeystoreKeyProvider;
import com.emc.ecs.sync.config.filter.DecryptionConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

public class DecryptionFilter extends AbstractFilter<DecryptionConfig> {
    private static final Logger log = LoggerFactory.getLogger(DecryptionFilter.class);

    private KeyProvider keyProvider;
    private CodecChain decodeChain;

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        try {
            if (keyProvider == null) {
                if (config.getDecryptKeystore() == null) throw new ConfigurationException("Must specify a keystore");
                if (config.getDecryptKeystore() == null)
                    throw new ConfigurationException("Must specify the master key alias");

                // Init keystore
                KeyStore keystore = KeyStore.getInstance("jks");
                keystore.load(new FileInputStream(config.getDecryptKeystore()), config.getDecryptKeystorePass().toCharArray());
                log.info("Keystore Loaded");

                // TODO: should KeystoreKeyProvider require an alias even if decrypting?
                Enumeration<String> aliases = keystore.aliases();
                if (aliases == null || !aliases.hasMoreElements())
                    throw new ConfigurationException("keystore has no aliases");

                keyProvider = new KeystoreKeyProvider(keystore, config.getDecryptKeystorePass().toCharArray(),
                        aliases.nextElement());
            }

            decodeChain = new CodecChain(new EncryptionCodec()).withProperty(EncryptionCodec.PROP_KEY_PROVIDER, keyProvider);
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Decryption is based on the Atmos Java SDK encryption standard (https://community.emc.com/docs/DOC-34465).
     */
    @Override
    public void filter(ObjectContext objectContext) {
        ObjectMetadata metadata = objectContext.getObject().getMetadata();

        if (metadata.isDirectory()) {

            // we can only decrypt data objects
            log.debug("skipping directory " + objectContext.getSourceSummary().getIdentifier());
            getNext().filter(objectContext);

        } else {

            // get modifiable view of user metadata
            Map<String, String> metaView = metadata.getUserMetadataValueMap();
            InputStream dataStream = objectContext.getObject().getDataStream();

            String encodeSpec = metaView.get(CodecChain.META_TRANSFORM_MODE);
            if (encodeSpec != null) {
                // simply getting the decode stream will nuke the encryption metadata, so be sure to read any fields beforehand
                // update size
                String decryptedSize = metaView.get(EncryptionConstants.META_ENCRYPTION_UNENC_SIZE);
                if (decryptedSize == null)
                    throw new RuntimeException("encrypted object missing metadata field: " + EncryptionConstants.META_ENCRYPTION_UNENC_SIZE);
                metadata.setContentLength(Long.parseLong(decryptedSize));

                // change the object's data stream to be the encrypted stream
                objectContext.getObject().setDataStream(decodeChain.getDecodeStream(dataStream, metaView));

                // remove any checksum from the metadata, as it will be invalid after decryption
                metadata.setChecksum(null);

                // update mtime if necessary
                if (config.isDecryptUpdateMtime()) metadata.setModificationTime(new Date());

                getNext().filter(objectContext);
            } else {

                // object is not encrypted
                if (config.isFailIfNotEncrypted()) {
                    throw new RuntimeException("object is not encrypted");
                } else {

                    // pass on untouched
                    log.info("object {} is not encrypted; passing through untouched...", objectContext.getSourceSummary().getIdentifier());
                    getNext().filter(objectContext);
                }
            }
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not yet support reverse filters (verification)");
    }

    public KeyProvider getKeyProvider() {
        return keyProvider;
    }

    public void setKeyProvider(KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
    }
}
