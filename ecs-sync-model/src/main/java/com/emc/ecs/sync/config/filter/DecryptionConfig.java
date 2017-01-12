/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "decrypt")
@Label("Decryption Filter")
@Documentation("Decrypts object data using the Atmos Java SDK encryption standard (https://community.emc.com/docs/DOC-34465). " +
        "This method uses envelope encryption where each object has its own symmetric key that is itself " +
        "encrypted using the master asymmetric key. As such, there are additional metadata fields added to the " +
        "object that are required for decrypting")
public class DecryptionConfig extends AbstractConfig {
    private String decryptKeystore;
    private String decryptKeystorePass;
    private boolean failIfNotEncrypted;
    private boolean decryptUpdateMtime;

    @Option(orderIndex = 10, required = true, valueHint = "keystore-file",
            description = "required. the .jks keystore file that holds the decryption keys. which key to use is actually stored in the object metadata")
    public String getDecryptKeystore() {
        return decryptKeystore;
    }

    public void setDecryptKeystore(String decryptKeystore) {
        this.decryptKeystore = decryptKeystore;
    }

    @Option(orderIndex = 20, valueHint = "keystore-password", description = "the keystore password")
    public String getDecryptKeystorePass() {
        return decryptKeystorePass;
    }

    public void setDecryptKeystorePass(String decryptKeystorePass) {
        this.decryptKeystorePass = decryptKeystorePass;
    }

    @Option(orderIndex = 30, advanced = true, description = "by default, if an object is not encrypted, it will be passed through the filter chain untouched. set this flag to fail the object if it is not encrypted")
    public boolean isFailIfNotEncrypted() {
        return failIfNotEncrypted;
    }

    public void setFailIfNotEncrypted(boolean failIfNotEncrypted) {
        this.failIfNotEncrypted = failIfNotEncrypted;
    }

    @Option(orderIndex = 40, advanced = true, description = "by default, the modification time (mtime) of an object does not change when decrypted. set this flag to update the mtime. useful for in-place decryption when objects would not otherwise be overwritten due to matching timestamps")
    public boolean isDecryptUpdateMtime() {
        return decryptUpdateMtime;
    }

    public void setDecryptUpdateMtime(boolean decryptUpdateMtime) {
        this.decryptUpdateMtime = decryptUpdateMtime;
    }
}
