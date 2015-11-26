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

import com.emc.ecs.sync.model.object.DecryptedSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.ConfigurationException;
import com.emc.ecs.sync.util.TransformUtil;
import com.emc.vipr.transform.InputTransform;
import com.emc.vipr.transform.TransformFactory;
import com.emc.vipr.transform.encryption.KeyStoreEncryptionFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;

public class DecryptionFilter extends SyncFilter {
    private static final Logger log = LoggerFactory.getLogger(DecryptionFilter.class);

    public static final String ACTIVATION_NAME = "decrypt";

    public static final String KEYSTORE_FILE_OPTION = "decrypt-keystore";
    public static final String KEYSTORE_FILE_DESC = "required. the .jks keystore file that holds the decryption keys. which key to use is actually stored in the object metadata.";
    public static final String KEYSTORE_FILE_ARG_NAME = "keystore-file";

    public static final String KEYSTORE_PASS_OPTION = "decrypt-keystore-pass";
    public static final String KEYSTORE_PASS_DESC = "required. the keystore password.";
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
        opts.addOption(Option.builder().longOpt(KEYSTORE_FILE_OPTION).desc(KEYSTORE_FILE_DESC)
                .hasArg().argName(KEYSTORE_FILE_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(KEYSTORE_PASS_OPTION).desc(KEYSTORE_PASS_DESC)
                .hasArg().argName(KEYSTORE_PASS_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(FAIL_NOT_ENCRYPTED_OPTION).desc(FAIL_NOT_ENCRYPTED_DESC).build());
        opts.addOption(Option.builder().longOpt(UPDATE_MTIME_OPTION).desc(UPDATE_MTIME_DESC).build());
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
                log.info("Keystore Loaded");
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
     * Decryption is based on the Atmos Java SDK encryption standard (https://community.emc.com/docs/DOC-34465).
     */
    @Override
    public void filter(SyncObject obj) {
        if (obj.isDirectory()) {
            // we can only decrypt data objects
            log.debug("skipping directory " + obj);
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
                    log.debug("transform factory cannot decode " + transformSpec);

                    // object is not encrypted
                    if (failIfNotEncrypted) {
                        throw new RuntimeException("object is not encrypted");
                    } else {

                        // pass on untouched
                        log.info("object " + obj + " is not encrypted; passing through untouched...");
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
    public SyncObject reverseFilter(SyncObject obj) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not yet support reverse filters (verification)");
    }

    @Override
    public String getName() {
        return "Decryption Filter";
    }

    @Override
    public String getDocumentation() {
        return "Decrypts object data using the Atmos Java SDK encryption standard (https://community.emc.com/docs/DOC-34465). " +
                "This method uses envelope encryption where each object has its own symmetric key that is itself " +
                "encrypted using the master asymmetric key. As such, there are additional metadata fields added to the " +
                "object that are required for decrypting.";
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
}
