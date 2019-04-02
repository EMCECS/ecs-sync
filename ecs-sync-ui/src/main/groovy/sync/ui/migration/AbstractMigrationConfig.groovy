/*
 * Copyright 2013-2018 EMC Corporation. All Rights Reserved.
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
package sync.ui.migration

import grails.validation.Validateable
import groovy.transform.EqualsAndHashCode
import sync.ui.migration.a2e.A2EMigrationConfig
import sync.ui.storage.StorageEntry

import javax.xml.bind.annotation.*

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso([A2EMigrationConfig.class])
@EqualsAndHashCode(includes = 'guid')
abstract class AbstractMigrationConfig implements Validateable {
    @XmlElement
    String guid
    @XmlElement
    String sourceXmlKey
    @XmlElement
    String targetXmlKey

    abstract sourceStorageType, targetStorageType
    abstract shortName, description

    static constraints = {
        sourceXmlKey blank: false, validator: { val, obj ->
            try {
                new StorageEntry([xmlKey: val]).type == obj.sourceStorageType
            } catch (e) {
                e.message
            }
        }
        targetXmlKey blank: false, validator: { val, obj ->
            try {
                new StorageEntry([xmlKey: val]).type == obj.targetStorageType
            } catch (e) {
                e.message
            }
        }
    }
}
