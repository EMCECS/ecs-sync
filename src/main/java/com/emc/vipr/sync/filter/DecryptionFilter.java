package com.emc.vipr.sync.filter;

import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.TransformUtil;
import com.emc.vipr.transform.InputTransform;
import com.emc.vipr.transform.TransformConstants;
import com.emc.vipr.transform.TransformFactory;
import com.emc.vipr.transform.encryption.KeyStoreEncryptionFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

public class DecryptionFilter extends SyncFilter {
    private static final Logger l4j = Logger.getLogger(DecryptionFilter.class);

    public static final String ACTIVATION_NAME = "decrypt";

    public static final String KEYSTORE_FILE_OPTION = "decrypt-keystore";
    public static final String KEYSTORE_FILE_DESC = "the .jks keystore file that holds the decryption keys. which key to use is actually stored in the object metadata.";
    public static final String KEYSTORE_FILE_ARG_NAME = "keystore-file";

    public static final String KEYSTORE_PASS_OPTION = "decrypt-keystore-pass";
    public static final String KEYSTORE_PASS_DESC = "the keystore password.";
    public static final String KEYSTORE_PASS_ARG_NAME = "keystore-password";

    public static final String FAIL_NOT_ENCRYPTED_OPTION = "fail-if-not-encrypted";
    public static final String FAIL_NOT_ENCRYPTED_DESC = "by default, if an object is not encrypted, it will be passed through the filter chain untouched. set this flag to fail the object if it is not encrypted.";

    public static final String UPDATE_MTIME_OPTION = "decrypt-update-mtime";
    public static final String UPDATE_MTIME_DESC = "by default, the modification time (mtime) of an object does not change when decrypted. set this flag to update the mtime. useful for in-place decryption when objects would not otherwise be overwritten due to matching timestamps.";

    private String keystoreFile;
    private String keystorePass;
    private boolean failIfNotEncrypted;
    private boolean updateMtime;

    private KeyStore keystore;
    private TransformFactory transformFactory;

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
        opts.addOption(new OptionBuilder().withLongOpt(FAIL_NOT_ENCRYPTED_OPTION).withDescription(FAIL_NOT_ENCRYPTED_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(UPDATE_MTIME_OPTION).withDescription(UPDATE_MTIME_DESC).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        keystoreFile = line.getOptionValue(KEYSTORE_FILE_OPTION);
        keystorePass = line.getOptionValue(KEYSTORE_PASS_OPTION);
        failIfNotEncrypted = line.hasOption(FAIL_NOT_ENCRYPTED_OPTION);
        updateMtime = line.hasOption(UPDATE_MTIME_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        try {
            if (keystore == null) {
                if (keystoreFile == null) throw new ConfigurationException("Must specify a keystore");

                // Init keystore
                keystore = KeyStore.getInstance("jks");
                keystore.load(new FileInputStream(keystoreFile), keystorePass.toCharArray());
                l4j.info("Keystore Loaded");
            }

            // TODO: remove alias logic when decryption factory no longer requires an alias
            Enumeration<String> aliases = keystore.aliases();
            if (aliases == null || !aliases.hasMoreElements())
                throw new ConfigurationException("keystore has no aliases");
            transformFactory = new KeyStoreEncryptionFactory(keystore, aliases.nextElement(), keystorePass.toCharArray());

        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    /**
     * Decryption is based on the ViPR object SDK encryption standard (https://community.emc.com/docs/DOC-34465).
     */
    @Override
    public void filter(SyncObject<?> obj) {
        if (obj.isDirectory()) {
            // we can only decrypt data objects
            l4j.debug("skipping directory " + obj);
            getNext().filter(obj);
        } else {
            try {
                Map<String, String> metadata = obj.getMetadata().getUserMetadataValueMap();

                // pull the last transform spec from the object metadata (must be the last, which is the next to undo)
                String transformSpec = TransformUtil.getLastTransform(obj);

                if (transformSpec != null && transformFactory.canDecode(transformSpec, metadata)) {

                    // create the transformer
                    InputTransform transform = transformFactory.getInputTransform(transformSpec, obj.getInputStream(), metadata);

                    // update mtime if necessary
                    if (updateMtime) obj.getMetadata().setModificationTime(new Date());

                    // wrap object with decrypted stream and pass on to target
                    getNext().filter(new DecryptedSyncObject(obj, transform));
                } else {
                    l4j.debug("transform factory cannot decode " + transformSpec);

                    // object is not encrypted
                    if (failIfNotEncrypted) {
                        throw new RuntimeException("object is not encrypted");
                    } else {

                        // pass on untouched
                        l4j.info("object " + obj + " is not encrypted; passing through untouched...");
                        getNext().filter(obj);
                    }
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
        return "Decryption Filter";
    }

    @Override
    public String getDocumentation() {
        return "Decrypts object data using the ViPR SDK encryption standard (https://community.emc.com/docs/DOC-34465). " +
                "This method uses envelope encryption where each object has its own symmetric key that is itself " +
                "encrypted using the master asymmetric key. As such, there are additional metadata fields added to the " +
                "object that are required for decrypting. All options below are required.";
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

    public boolean isFailIfNotEncrypted() {
        return failIfNotEncrypted;
    }

    public void setFailIfNotEncrypted(boolean failIfNotEncrypted) {
        this.failIfNotEncrypted = failIfNotEncrypted;
    }

    public KeyStore getKeystore() {
        return keystore;
    }

    public void setKeystore(KeyStore keystore) {
        this.keystore = keystore;
    }

    public boolean isUpdateMtime() {
        return updateMtime;
    }

    public void setUpdateMtime(boolean updateMtime) {
        this.updateMtime = updateMtime;
    }

    private class DecryptedSyncObject extends SyncObject {
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
}
