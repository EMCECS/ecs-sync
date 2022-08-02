/*
 * Copyright (c) 2018-2019 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui.migration.a2e

import sync.ui.migration.AbstractMigrationConfig
import sync.ui.storage.StorageEntry

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
class A2EMigrationConfig extends AbstractMigrationConfig {
    def sourceStorageType = 'atmos', targetStorageType = 'ecs'
    @Lazy
    volatile targetName = new StorageEntry([xmlKey: targetXmlKey]).name
    @Lazy
    volatile shortName = "${atmosSubtenantName.take(10)}_to_${targetName.take(10)}_${selectedUsers.size()}_users"
    @Lazy
    volatile description = "${atmosSubtenantName} -> ${targetName} (${selectedUsers.collect { "${userNamespaceMap[it]}/${it}" }.join(', ')})"
    @XmlElement
    String atmosSubtenantName
    @XmlElement
    String replicationGroup
    @XmlElement
    List<String> selectedUsers = []
    @XmlElement
    Map<String, String> userMap = [:]
    @XmlElement
    Map<String, String> userNamespaceMap = [:]
    @XmlElement
    int staleMpuThresholdDays = 7
    @XmlElement
    boolean useS3Proxy
    @XmlElement
    String s3ProxyBucketEndpoint
    @XmlElement
    boolean s3ProxyBucketSmartClient = true
    @XmlElement
    String s3ProxyBucketAccessKey
    @XmlElement
    String s3ProxyBucketSecretKey
    @XmlElement
    String s3ProxyBucket
    @XmlElement
    int s3ProxyPollIntervalSecs = 30
    @XmlElement
    int s3ProxyPreBucketMigrationWaitSecs = 60

    static constraints = {
        atmosSubtenantName blank: false
        replicationGroup blank: false
        selectedUsers validator: { val ->
            val.removeAll([null])
            val.size() > 0
        }
        userMap validator: { val -> val.size() > 0 }
        userNamespaceMap validator: { val -> val.size() > 0 }

        s3ProxyBucketEndpoint nullable: true, validator: { val, obj -> !obj.useS3Proxy || val?.trim() as boolean }
        s3ProxyBucketAccessKey nullable: true, validator: { val, obj -> !obj.useS3Proxy || val?.trim() as boolean }
        s3ProxyBucketSecretKey nullable: true, validator: { val, obj -> !obj.useS3Proxy || val?.trim() as boolean }
        s3ProxyBucket nullable: true, validator: { val, obj -> !obj.useS3Proxy || val?.trim() as boolean }
    }
}
