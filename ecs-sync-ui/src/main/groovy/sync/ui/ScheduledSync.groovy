package sync.ui

import com.emc.ecs.sync.rest.SyncConfig
import grails.validation.Validateable

import javax.xml.bind.annotation.*

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
class ScheduledSync implements Validateable {
    @XmlElement
    List<Day> daysOfWeek = []
    @XmlElement
    int startHour
    @XmlElement
    int startMinute
    @XmlElement
    SyncConfig config = new SyncConfig()
    @XmlElement
    AlertConfig alerts = new AlertConfig()

    @XmlAccessorType(XmlAccessType.NONE)
    static class AlertConfig {
        @XmlElement
        boolean onStart
        @XmlElement
        boolean onComplete
        @XmlElement
        boolean onError
    }

    @XmlEnum
    static enum Day {
        Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
    }

    static constraints = {
        daysOfWeek size: 1..7
    }
}
