package sync.ui

import grails.validation.Validateable

class ScheduleEntry implements Validateable {
    static String prefix = "schedule/"

    static List<ScheduleEntry> list(EcsService ecsService) {
        ecsService.listConfigObjects(prefix).collect {
            new ScheduleEntry([ecsService: ecsService, xmlKey: it])
        }.sort { a, b -> a.name <=> b.name }
    }

    EcsService ecsService

    String name
    @Lazy
    String xmlKey = "${prefix}${name}.xml"
    @Lazy
    def allKeys = [xmlKey]
    @Lazy(soft = true)
    ScheduledSync scheduledSync = exists() ? ecsService.readConfigObject(xmlKey, ScheduledSync.class) : new ScheduledSync()

    boolean exists() {
        return (name && ecsService && ecsService.configObjectExists(xmlKey))
    }

    def write() {
        ecsService.writeConfigObject(xmlKey, scheduledSync, 'application/xml')
    }

    def setXmlKey(String key) {
        this.name = key.split('/').last().replaceFirst(/[.]xml$/, '')
    }

    static constraints = {
        name blank: false
        scheduledSync validator: { it.validate() ?: 'invalid' }
    }
}
