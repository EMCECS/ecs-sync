/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "encrypt")
@Label("Encryption Filter")
@Documentation("Encrypts object data using the Atmos Java SDK encryption standard (https://community.emc.com/docs/DOC-34465). " +
        "This method uses envelope encryption where each object has its own symmetric key that is itself " +
        "encrypted using the master asymmetric key. As such, there are additional metadata fields added to the " +
        "object that are required for decrypting. Note that currently, metadata is not encrypted")
public class EncryptionConfig extends AbstractConfig {
    private String encryptKeystore;
    private String encryptKeystorePass;
    private String encryptKeyAlias;
    private boolean encryptForceStrong;
    private boolean failIfEncrypted;
    private boolean encryptUpdateMtime;

    @Option(orderIndex = 10, required = true, valueHint = "keystore-file",
            description = "the .jks keystore file that holds the master encryption key")
    public String getEncryptKeystore() {
        return encryptKeystore;
    }

    public void setEncryptKeystore(String encryptKeystore) {
        this.encryptKeystore = encryptKeystore;
    }

    @Option(orderIndex = 20, valueHint = "keystore-password", description = "the keystore password", sensitive = true)
    public String getEncryptKeystorePass() {
        return encryptKeystorePass;
    }

    public void setEncryptKeystorePass(String encryptKeystorePass) {
        this.encryptKeystorePass = encryptKeystorePass;
    }

    @Option(orderIndex = 30, required = true, description = "the alias of the master encryption key within the keystore")
    public String getEncryptKeyAlias() {
        return encryptKeyAlias;
    }

    public void setEncryptKeyAlias(String encryptKeyAlias) {
        this.encryptKeyAlias = encryptKeyAlias;
    }

    @Option(orderIndex = 40, description = "256-bit cipher strength is always used if available. this option will stop operations if strong ciphers are not available")
    public boolean isEncryptForceStrong() {
        return encryptForceStrong;
    }

    public void setEncryptForceStrong(boolean encryptForceStrong) {
        this.encryptForceStrong = encryptForceStrong;
    }

    @Option(orderIndex = 50, advanced = true, description = "by default, if an object is already encrypted using this method, it will be passed through the filter chain untouched. set this flag to fail the object if it is already encrypted")
    public boolean isFailIfEncrypted() {
        return failIfEncrypted;
    }

    public void setFailIfEncrypted(boolean failIfEncrypted) {
        this.failIfEncrypted = failIfEncrypted;
    }

    @Option(orderIndex = 60, advanced = true, description = "by default, the modification time (mtime) of an object does not change when encrypted. set this flag to update the mtime. useful for in-place encryption when objects would not otherwise be overwritten due to matching timestamps")
    public boolean isEncryptUpdateMtime() {
        return encryptUpdateMtime;
    }

    public void setEncryptUpdateMtime(boolean encryptUpdateMtime) {
        this.encryptUpdateMtime = encryptUpdateMtime;
    }
}
