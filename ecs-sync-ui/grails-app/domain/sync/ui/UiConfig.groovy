package sync.ui

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
class UiConfig {
    @XmlElement
    ConfigStorageType configStorageType = ConfigStorageType.LocalDisk
    @XmlElement
    String filePath = '/opt/emc/ecs-sync/config'
    @XmlElement
    String hosts
    @XmlElement
    String protocol = 'HTTP'
    @XmlElement
    int port = 9020
    @XmlElement
    String accessKey
    @XmlElement
    String secretKey
    @XmlElement
    String configBucket = 'ecs-sync'
    @XmlElement
    boolean autoArchive
    @XmlElement
    String alertEmail

    static constraints = {
        filePath nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.LocalDisk || val }
        hosts nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.ECS || val }
        accessKey nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.ECS || val }
        secretKey nullable: true, validator: { val, obj -> obj.configStorageType != ConfigStorageType.ECS || val }
    }
}
