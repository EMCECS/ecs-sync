package sync.ui

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
class UiConfig {
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
    @XmlElement
    Map<String, String> defaults = [:]
}
