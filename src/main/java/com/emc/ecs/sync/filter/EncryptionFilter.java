/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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

import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.AbstractSyncObject;
import com.emc.ecs.sync.model.object.DecryptedSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.ConfigurationException;
import com.emc.ecs.sync.util.TransformUtil;
import com.emc.vipr.transform.InputTransform;
import com.emc.vipr.transform.OutputTransform;
import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.encryption.EncryptionTransformFactory;
import com.emc.vipr.transform.encryption.KeyStoreEncryptionFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class EncryptionFilter extends SyncFilter {
    private static final Logger log = LoggerFactory.getLogger(EncryptionFilter.class);

    public static final String ACTIVATION_NAME = "encrypt";

    public static final String KEYSTORE_FILE_OPTION = "encrypt-keystore";
    public static final String KEYSTORE_FILE_DESC = "required. the .jks keystore file that holds the master encryption key.";
    public static final String KEYSTORE_FILE_ARG_NAME = "keystore-file";

    public static final String KEYSTORE_PASS_OPTION = "encrypt-keystore-pass";
    public static final String KEYSTORE_PASS_DESC = "required. the keystore password.";
    public static final String KEYSTORE_PASS_ARG_NAME = "keystore-password";

    public static final String KEY_ALIAS_OPTION = "encrypt-key-alias";
    public static final String KEY_ALIAS_DESC = "required. the alias of the master encryption key within the keystore.";
    public static final String KEY_ALIAS_ARG_NAME = "key-alias";

    public static final String FORCE_STRONG_OPTION = "encrypt-force-strong";
    public static final String FORCE_STRONG_DESC = "forces 256-bit cipher strength";

    public static final String FAIL_ENCRYPTED_OPTION = "fail-if-encrypted";
    public static final String FAIL_ENCRYPTED_DESC = "by default, if an object is already encrypted using this method, it will be passed through the filter chain untouched. set this flag to fail the object if it is already encrypted.";

    public static final String UPDATE_MTIME_OPTION = "encrypt-update-mtime";
    public static final String UPDATE_MTIME_DESC = "by default, the modification time (mtime) of an object does not change when encrypted. set this flag to update the mtime. useful for in-place encryption when objects would not otherwise be overwritten due to matching timestamps.";

    private String keystoreFile;
    private String keystorePass;
    private String keyAlias;
    private boolean forceStrong;
    private boolean failIfEncrypted;
    private boolean updateMtime;

    private KeyStore keystore;
    private EncryptionTransformFactory transformFactory;

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(KEYSTORE_FILE_OPTION).desc(KEYSTORE_FILE_DESC)
                .hasArg().argName(KEYSTORE_FILE_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(KEYSTORE_PASS_OPTION).desc(KEYSTORE_PASS_DESC)
                .hasArg().argName(KEYSTORE_PASS_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(KEY_ALIAS_OPTION).desc(KEY_ALIAS_DESC)
                .hasArg().argName(KEY_ALIAS_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(FORCE_STRONG_OPTION).desc(FORCE_STRONG_DESC).build());
        opts.addOption(Option.builder().longOpt(FAIL_ENCRYPTED_OPTION).desc(FAIL_ENCRYPTED_DESC).build());
        opts.addOption(Option.builder().longOpt(UPDATE_MTIME_OPTION).desc(UPDATE_MTIME_DESC).build());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        keystoreFile = line.getOptionValue(KEYSTORE_FILE_OPTION);
        keystorePass = line.getOptionValue(KEYSTORE_PASS_OPTION);
        keyAlias = line.getOptionValue(KEY_ALIAS_OPTION);
        forceStrong = line.hasOption(FORCE_STRONG_OPTION);
        failIfEncrypted = line.hasOption(FAIL_ENCRYPTED_OPTION);
        updateMtime = line.hasOption(UPDATE_MTIME_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (keyAlias == null) throw new ConfigurationException("Must specify the master key alias");

        try {
            if (keystore == null) {
                if (keystoreFile == null) throw new ConfigurationException("Must specify a keystore");

                // Init keystore
                keystore = KeyStore.getInstance("jks");
                keystore.load(new FileInputStream(keystoreFile), keystorePass.toCharArray());
                log.info("Keystore Loaded");
            }

            transformFactory = new KeyStoreEncryptionFactory(keystore, keyAlias, keystorePass.toCharArray());

            // check if we have an unlimited strength policy available
            if (EncryptionTransformFactory.getMaxKeySize(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM) >= 256) {
                log.info("using 256-bit cipher strength");
                transformFactory.setEncryptionSettings(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM, 256, null);
            } else {
                if (forceStrong) throw new ConfigurationException("strong encryption is not available");
                log.warn("high-strength encryption is unavailable; defaulting to 128-bit");
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
    public void filter(SyncObject obj) {
        if (obj.isDirectory()) {
            // we can only encrypt data objects
            log.debug("skipping directory " + obj);
            getNext().filter(obj);
        } else {
            try {
                // see if the object is already encrypted via this method (encrypting again will overwrite the metadata
                // fields and make the object unreadable)
                String encryptSpec = TransformUtil.getEncryptionSpec(obj);
                if (encryptSpec != null) {
                    if (failIfEncrypted) {
                        throw new UnsupportedOperationException("object is already encrypted (" + encryptSpec + ")");
                    } else {

                        // pass on untouched
                        log.info("object {} is already encrypted ({}); passing through untouched...",
                                obj, encryptSpec);
                        getNext().filter(obj);
                    }
                } else {

                    // create the transformer
                    OutputTransform transform = transformFactory.getOutputTransform(obj.getInputStream(),
                            obj.getMetadata().getUserMetadataValueMap());

                    // update mtime if necessary
                    if (updateMtime) obj.getMetadata().setModificationTime(new Date());

                    // wrap object with encrypted stream and pass on to target
                    getNext().filter(new EncryptedSyncObject(obj, transform));
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @see DecryptionFilter#filter(SyncObject)
     */
    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        obj = getNext().reverseFilter(obj);
        if (obj.isDirectory()) return obj;
        try {
            String transformSpec = TransformUtil.getLastTransform(obj);
            InputTransform transform = transformFactory.getInputTransform(transformSpec, obj.getInputStream(),
                    obj.getMetadata().getUserMetadataValueMap());
            return new DecryptedSyncObject(obj, transform);
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException("Decryption failed in reverse-filter (verification)", e);
        }
    }

    @Override
    public String getName() {
        return "Encryption Filter";
    }

    @Override
    public String getDocumentation() {
        return "Encrypts object data using the Atmos Java SDK encryption standard (https://community.emc.com/docs/DOC-34465). " +
                "This method uses envelope encryption where each object has its own symmetric key that is itself " +
                "encrypted using the master asymmetric key. As such, there are additional metadata fields added to the " +
                "object that are required for decrypting. Note that currently, metadata is not encrypted.";
    }

    public String getKeystoreFile() {
        return keystoreFile;
    }

    public void setKeystoreFile(String keystoreFile) {
        this.keystoreFile = keystoreFile;
    }

    public String getKeystorePass() {
        return keystorePass;
    }

    public void setKeystorePass(String keystorePass) {
        this.keystorePass = keystorePass;
    }

    public String getKeyAlias() {
        return keyAlias;
    }

    public void setKeyAlias(String keyAlias) {
        this.keyAlias = keyAlias;
    }

    public KeyStore getKeystore() {
        return keystore;
    }

    public void setKeystore(KeyStore keystore) {
        this.keystore = keystore;
    }

    public boolean isForceStrong() {
        return forceStrong;
    }

    public void setForceStrong(boolean forceStrong) {
        this.forceStrong = forceStrong;
    }

    public boolean isFailIfEncrypted() {
        return failIfEncrypted;
    }

    public void setFailIfEncrypted(boolean failIfEncrypted) {
        this.failIfEncrypted = failIfEncrypted;
    }

    public boolean isUpdateMtime() {
        return updateMtime;
    }

    public void setUpdateMtime(boolean updateMtime) {
        this.updateMtime = updateMtime;
    }

    public static class EncryptedSyncObject extends AbstractSyncObject {
        private SyncObject delegate;
        private OutputTransform transform;
        private boolean metadataComplete = false;

        @SuppressWarnings("unchecked")
        public EncryptedSyncObject(SyncObject delegate, OutputTransform transform) {
            super(delegate.getParentPlugin(), delegate.getRawSourceIdentifier(), delegate.getSourceIdentifier(),
                    delegate.getRelativePath(), delegate.isDirectory());
            this.delegate = delegate;
            this.transform = transform;

            // predict the encrypted size
            long encryptedSize = (delegate.getMetadata().getContentLength() / 16L + 1L) * 16L;
            delegate.getMetadata().setContentLength(encryptedSize);
        }

        @Override
        protected void loadObject() {
            // calling getMetadata() will call this method on delegate
        }

        @Override
        protected InputStream createSourceInputStream() {
            return transform.getEncodedInputStream();
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

                    // add metadata specific to the transform
                    Map<String, String> encryptedMetadata = transform.getEncodedMetadata();
                    for (String key : encryptedMetadata.keySet()) {
                        delegate.getMetadata().setUserMetadataValue(key, encryptedMetadata.get(key));
                    }

                    // append this transformation to the transform spec
                    String transformSpec = delegate.getMetadata().getUserMetadataValue(TransformConstants.META_TRANSFORM_MODE);
                    if (transformSpec == null) transformSpec = "";
                    else transformSpec += "|";
                    transformSpec += transform.getTransformConfig();
                    delegate.getMetadata().setUserMetadataValue(TransformConstants.META_TRANSFORM_MODE, transformSpec);

                    metadataComplete = true;
                } catch (IllegalStateException e) {
                    log.debug("could not get encoded metadata - assuming object has not been streamed", e);
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
}
