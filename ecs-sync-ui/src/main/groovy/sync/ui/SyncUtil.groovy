package sync.ui

import com.emc.ecs.sync.rest.SyncConfig
import groovy.util.slurpersupport.GPathResult

class SyncUtil {
    static String getSourcePath(GPathResult syncConfig) {
        def sourcePath = syncConfig.Source.Property.find { it.@name == 'rootFile' }?.@value?.toString()
        if (!sourcePath)
            sourcePath = syncConfig.Source.Property.find { it.@name == 'bucketName' }?.@value?.toString() + '/' +
                    syncConfig.Source.Property.find { it.@name == 'rootKey' }?.@value?.toString()
        return sourcePath
    }

    static String getSourcePath(SyncConfig syncConfig) {
        def sourcePath = syncConfig.source.customProperties.find { it.key == 'rootFile' }?.value
        if (!sourcePath)
            sourcePath = syncConfig.source.customProperties.find { it.key == 'bucketName' }?.value + '/' +
                    syncConfig.source.customProperties.find { it.key == 'rootKey' }?.value
        return sourcePath
    }

    static String getTargetPath(GPathResult syncConfig) {
        def targetPath = syncConfig.Target.Property.find { it.@name == 'targetRoot' }?.@value?.toString()
        if (!targetPath)
            targetPath = syncConfig.Target.Property.find { it.@name == 'bucketName' }?.@value?.toString() + '/' +
                    syncConfig.Target.Property.find { it.@name == 'rootKey' }?.@value?.toString()
        return targetPath
    }

    static String getTargetPath(SyncConfig syncConfig) {
        def targetPath = syncConfig.target.customProperties.find { it.key == 'targetRoot' }?.value
        if (!targetPath)
            targetPath = syncConfig.target.customProperties.find { it.key == 'bucketName' }?.value + '/' +
                    syncConfig.target.customProperties.find { it.key == 'rootKey' }?.value
        return targetPath
    }

    static correctBindingResult(SyncConfig syncConfig) {
        syncConfig.source?.customListProperties = syncConfig.source?.customListProperties?.collectEntries {
            [it.key, it.value instanceof String ? [it.value] : it.value]
        }
        syncConfig.target?.customListProperties = syncConfig.target?.customListProperties?.collectEntries {
            [it.key, it.value instanceof String ? [it.value] : it.value]
        }
    }

    static configureDatabase(syncConfig, dbType, dbName, grailsApplication) {
        if (dbType && dbType != 'None') {
            if (dbType == 'Sqlite') {
                def dbDir = grailsApplication.config.sync.dbDir
                if (dbDir) syncConfig.dbFile = "${dbDir}/${dbName}.db"
                else throw new RuntimeException('dbDir.missing')
            } else if (dbType == 'mySQL') {
                def dbConnectString = grailsApplication.config.sync.dbConnectString
                if (dbConnectString) {
                    syncConfig.dbConnectString = dbConnectString
                    syncConfig.dbTable = dbName
                } else throw new RuntimeException('dbConnectString.missing')
            }
        }
    }
}
