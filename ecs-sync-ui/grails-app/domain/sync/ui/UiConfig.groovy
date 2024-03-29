/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package sync.ui

import groovy.transform.EqualsAndHashCode
import org.springframework.beans.factory.annotation.Value

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
@EqualsAndHashCode
class UiConfig implements EcsBucket {
    @XmlElement
    ConfigStorageType configStorageType = ConfigStorageType.LocalDisk
    @XmlElement
    @Value('${com.emc.ecs.sync.defaultConfigStorage:/opt/emc/ecs-sync/config}')
    String filePath
    @XmlElement
    String hosts
    @XmlElement
    String protocol = 'HTTP'
    @XmlElement
    int port = 9020
    @XmlElement
    boolean smartClient = true
    @XmlElement
    String accessKey
    @XmlElement
    String secretKey
    @XmlElement
    String configBucket = 'ecs-sync'
    @Override
    String getBucket() {
        return configBucket
    }
    @XmlElement
    boolean autoArchive
    @XmlElement
    String alertEmail

    static constraints = {
        filePath nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.LocalDisk || val }
        hosts nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.ECS || val }
        accessKey nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.ECS || val }
        secretKey nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.ECS || val }
        alertEmail nullable: true
    }
}
