/*
 * Copyright (c) 2024 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui.migration.e2e

import sync.ui.migration.AbstractMigrationConfig
import sync.ui.storage.StorageEntry

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

/**
 * ECS-to-ECS migration: reproduces selected namespaces (with their object users and buckets) on a target ECS and
 * copies all object data.
 *
 * The source and target storage entries may point at the <em>same</em> cluster, which - combined with the namespace
 * renaming performed by {@link com.emc.ecs.sync.cluster.NamespaceMapper} - is how data is migrated between storage
 * pools (replication groups) within one cluster.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
class E2EMigrationConfig extends AbstractMigrationConfig {
    def sourceStorageType = 'ecs', targetStorageType = 'ecs'

    @Lazy
    volatile sourceName = new StorageEntry([xmlKey: sourceXmlKey]).name
    @Lazy
    volatile targetName = new StorageEntry([xmlKey: targetXmlKey]).name
    @Lazy
    volatile shortName = "${sourceName.take(10)}_to_${targetName.take(10)}_${selectedNamespaces.size()}_ns"
    @Lazy
    volatile description = "${sourceName} -> ${targetName} (${selectedNamespaces.join(', ')})"

    /** Namespaces on the source cluster to migrate. */
    @XmlElement
    List<String> selectedNamespaces = []

    /**
     * Name of the replication group on the target that receives the data. This selects the destination storage
     * pool, so it is required.
     */
    @XmlElement
    String replicationGroup

    /**
     * Appended to a namespace name that already exists on the target; a counter is added if the suffixed name is also
     * taken. Object users in a renamed namespace are renamed with the same suffix.
     */
    @XmlElement
    String targetNamespaceSuffix = '_target'

    /** Threads used within each individual bucket copy job. */
    @XmlElement
    int threadCount = 16

    /** Verify each object after it is written. */
    @XmlElement
    boolean verify = true

    /** Copy all versions of objects in versioned buckets. */
    @XmlElement
    boolean includeVersions = true

    /**
     * The management API never discloses an existing user's secret key, so one is generated for any source user that
     * has none. This writes to the source cluster; disable to fail instead.
     */
    @XmlElement
    boolean generateSourceSecretKeys = true

    /** ECS management endpoints commonly present a self-signed certificate. */
    @XmlElement
    boolean trustSelfSignedCertificates = true

    static constraints = {
        replicationGroup blank: false
        targetNamespaceSuffix blank: false
        threadCount min: 1
        selectedNamespaces validator: { val ->
            val.removeAll([null, ''])
            val.size() > 0 ?: 'at least one namespace must be selected'
        }
    }
}
