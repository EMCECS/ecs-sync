package com.emc.vipr.sync.filter;

import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.TransformUtil;
import com.emc.vipr.transform.OutputTransform;
import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.encryption.EncryptionTransformFactory;
import com.emc.vipr.transform.encryption.KeyStoreEncryptionFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

public class EncryptionFilter extends SyncFilter {
    private static final Logger l4j = Logger.getLogger(EncryptionFilter.class);

    public static final String ACTIVATION_NAME = "encrypt";

    public static final String KEYSTORE_FILE_OPTION = "encrypt-keystore";
    public static final String KEYSTORE_FILE_DESC = "the .jks keystore file that holds the master encryption key.";
    public static final String KEYSTORE_FILE_ARG_NAME = "keystore-file";

    public static final String KEYSTORE_PASS_OPTION = "encrypt-keystore-pass";
    public static final String KEYSTORE_PASS_DESC = "the keystore password.";
    public static final String KEYSTORE_PASS_ARG_NAME = "keystore-password";

    public static final String KEY_ALIAS_OPTION = "encrypt-key-alias";
    public static final String KEY_ALIAS_DESC = "the alias of the master encryption key within the keystore.";
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
        opts.addOption(new OptionBuilder().withLongOpt(KEYSTORE_FILE_OPTION).withDescription(KEYSTORE_FILE_DESC)
                .hasArg().withArgName(KEYSTORE_FILE_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(KEYSTORE_PASS_OPTION).withDescription(KEYSTORE_PASS_DESC)
                .hasArg().withArgName(KEYSTORE_PASS_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(KEY_ALIAS_OPTION).withDescription(KEY_ALIAS_DESC)
                .hasArg().withArgName(KEY_ALIAS_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(FORCE_STRONG_OPTION).withDescription(FORCE_STRONG_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(FAIL_ENCRYPTED_OPTION).withDescription(FAIL_ENCRYPTED_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(UPDATE_MTIME_OPTION).withDescription(UPDATE_MTIME_DESC).create());
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
                l4j.info("Keystore Loaded");
            }

            transformFactory = new KeyStoreEncryptionFactory(keystore, keyAlias, keystorePass.toCharArray());

            // check if we have an unlimited strength policy available
            if (EncryptionTransformFactory.getMaxKeySize(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM) >= 256) {
                l4j.info("using 256-bit cipher strength");
                transformFactory.setEncryptionSettings(TransformConstants.DEFAULT_ENCRYPTION_TRANSFORM, 256, null);
            } else {
                if (forceStrong) throw new ConfigurationException("strong encryption is not available");
                l4j.warn("high-strength encryption is unavailable; defaulting to 128-bit");
            }

        } catch (ConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Encryption is based on the ViPR object SDK encryption standard (https://community.emc.com/docs/DOC-34465).
     */
    @Override
    public void filter(SyncObject<?> obj) {
        if (obj.isDirectory()) {
            // we can only encrypt data objects
            l4j.debug("skipping directory " + obj);
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
                        LogMF.info(l4j, "object {0} is already encrypted ({1}); passing through untouched...",
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

    @Override
    public String getName() {
        return "Encryption Filter";
    }

    @Override
    public String getDocumentation() {
        return "Encrypts object data using the ViPR SDK encryption standard (https://community.emc.com/docs/DOC-34465). " +
                "This method uses envelope encryption where each object has its own symmetric key that is itself " +
                "encrypted using the master asymmetric key. As such, there are additional metadata fields added to the " +
                "object that are required for decrypting. Note that currently, metadata is not encrypted. All options " +
                "below are required.";
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

    private class EncryptedSyncObject extends SyncObject {
        private SyncObject delegate;
        private OutputTransform transform;
        private boolean metadataComplete = false;

        @SuppressWarnings("unchecked")
        public EncryptedSyncObject(SyncObject delegate, OutputTransform transform) {
            super(delegate.getRawSourceIdentifier(), delegate.getSourceIdentifier(),
                    delegate.getRelativePath(), delegate.isDirectory());
            this.delegate = delegate;
            this.transform = transform;

            // predict the encrypted size
            long encryptedSize = (delegate.getMetadata().getSize() / 16L + 1L) * 16L;
            delegate.getMetadata().setSize(encryptedSize);
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
                    l4j.debug("could not get encoded metadata - assuming object has not been streamed", e);
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
