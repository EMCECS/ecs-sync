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
package sync.ui.storage

import groovy.transform.EqualsAndHashCode

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
@EqualsAndHashCode
class EcsStorage extends AbstractStorage {
    @XmlElement
    String managementEndpoint
    @XmlElement
    String sysAdminUser
    @XmlElement
    String sysAdminPassword
    @XmlElement
    String s3ApiEndpoint
    @XmlElement
    boolean s3SmartClient

    static constraints = {
        managementEndpoint blank: false, validator: { val ->
            try {
                new URL(val) != null
            } catch (e) {
                e.message
            }
        }
        sysAdminUser blank: false
        sysAdminPassword blank: false
        s3ApiEndpoint blank: false
    }
}
