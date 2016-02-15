package sync.ui

import com.emc.ecs.sync.rest.SyncConfig
import com.emc.ecs.sync.rest.SyncProgress

import javax.xml.bind.annotation.XmlAccessType
import javax.xml.bind.annotation.XmlAccessorType
import javax.xml.bind.annotation.XmlElement
import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
class SyncResult {
    @XmlElement
    SyncConfig config
    @XmlElement
    SyncProgress progress
}
