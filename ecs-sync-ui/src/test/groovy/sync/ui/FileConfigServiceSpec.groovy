package sync.ui

import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.config.SyncOptions
import com.emc.ecs.sync.config.storage.CasConfig
import grails.test.mixin.TestFor
import spock.lang.Specification

/**
 * See the API for {@link grails.test.mixin.services.ServiceUnitTestMixin} for usage instructions
 */
@TestFor(FileConfigService)
class FileConfigServiceSpec extends Specification {
    def XML = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
            '<syncConfig xmlns="http://www.emc.com/ecs/sync/model">' +
            '<options>' +
            '<bufferSize>524288</bufferSize>' +
            '<dbTable>cas_migration</dbTable>' +
            '<deleteSource>false</deleteSource>' +
            '<forceSync>false</forceSync>' +
            '<ignoreInvalidAcls>false</ignoreInvalidAcls>' +
            '<logLevel>quiet</logLevel>' +
            '<monitorPerformance>true</monitorPerformance>' +
            '<recursive>true</recursive>' +
            '<rememberFailed>false</rememberFailed>' +
            '<retryAttempts>2</retryAttempts>' +
            '<sourceListFile>/path/to/clip_list.lst</sourceListFile>' +
            '<syncAcl>false</syncAcl>' +
            '<syncData>true</syncData>' +
            '<syncMetadata>true</syncMetadata>' +
            '<syncRetentionExpiration>false</syncRetentionExpiration>' +
            '<threadCount>30</threadCount>' +
            '<timingWindow>1000</timingWindow>' +
            '<timingsEnabled>false</timingsEnabled>' +
            '<verify>true</verify>' +
            '<verifyOnly>false</verifyOnly>' +
            '</options>' +
            '<source>' +
            '<casConfig>' +
            '<applicationName>ECS-Sync</applicationName>' +
            '<connectionString>hpp://host1,host2?source.pea</connectionString>' +
            '<deleteReason>Deleted by ECS-Sync</deleteReason>' +
            '<uri>cas:hpp://host1,host2?source.pea</uri>' +
            '</casConfig>' +
            '</source>' +
            '<target>' +
            '<casConfig>' +
            '<applicationName>ECS-Sync</applicationName>' +
            '<connectionString>hpp://host1,host2?target.pea</connectionString>' +
            '<deleteReason>Deleted by ECS-Sync</deleteReason>' +
            '<uri>cas:hpp://host1,host2?target.pea</uri>' +
            '</casConfig>' +
            '</target>' +
            '</syncConfig>'

    def OBJ = new SyncConfig().withOptions(new SyncOptions().withThreadCount(30).withVerify(true)
            .withSourceListFile("/path/to/clip_list.lst").withDbTable("cas_migration"))
            .withSource(new CasConfig().withConnectionString("hpp://host1,host2?source.pea"))
            .withTarget(new CasConfig().withConnectionString("hpp://host1,host2?target.pea"))

    def setup() {
    }

    def cleanup() {
    }

    void "test unmarshall"() {
        given: "xml file"
        def file = File.createTempFile("foo", ".xml")
        file.deleteOnExit()
        file.write(XML)

        when: "readConfigObject is called"
        Object foo = service.readConfigObject(file.getPath(), SyncConfig.class)

        then: "object should be hydrated"
        foo instanceof SyncConfig
        foo.getOptions() != null
        foo.getSource() instanceof CasConfig
    }

    void "test read string"() {
        given: "file"
        def file = File.createTempFile("foo", ".xml")
        file.deleteOnExit()
        file.write(XML)

        when: "readConfigObject is called"
        Object foo = service.readConfigObject(file.getPath(), String.class)

        then: "object should be contents as string"
        foo instanceof String
        XML == foo
    }

    void "test read bytes"() {
        given: "file"
        def file = File.createTempFile("foo", ".xml")
        file.deleteOnExit()
        file.write(XML)

        when: "readConfigObject is called"
        Object foo = service.readConfigObject(file.getPath(), byte[].class)

        then: "object should be contents as byte[]"
        foo instanceof byte[]
        XML.bytes == foo
    }

    void "test marshall"() {
        given: "empty file"
        def file = File.createTempFile("foo", ".xml")
        file.deleteOnExit()

        when: "writeConfigObject is called"
        service.writeConfigObject(file.getPath(), OBJ, "application/xml")

        then: "file should have XML"
        file.text == XML
    }

    void "test write string"() {
        given: "empty file"
        def file = File.createTempFile("foo", ".xml")
        file.deleteOnExit()

        when: "writeConfigObject is called"
        service.writeConfigObject(file.getPath(), XML, "application/xml")

        then: "file should have string contents"
        file.text == XML
    }

    void "test write bytes"() {
        given: "empty file"
        def file = File.createTempFile("foo", ".xml")
        file.deleteOnExit()

        when: "writeConfigObject is called"
        service.writeConfigObject(file.getPath(), XML.bytes, "application/xml")

        then: "file should have XML"
        file.bytes == XML.bytes
    }
}
