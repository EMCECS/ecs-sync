package sync.ui

import com.emc.ecs.sync.config.ConfigUtil
import com.emc.ecs.sync.config.ConfigurationException
import com.emc.ecs.sync.config.SyncConfig
import grails.web.databinding.DataBinder
import grails.web.servlet.mvc.GrailsParameterMap
import org.springframework.validation.BindException

class SyncUtil {
    static String getLocation(storageConfig) {
        def wrapper = ConfigUtil.wrapperFor(storageConfig.getClass())
        def uri = wrapper.generateUri(storageConfig)

        // try to scrub credentials
        uri?.replaceFirst(/^(${wrapper.uriPrefix})?([a-zA-Z]*:\/\/)?(?:.*@)?(.*)$/, '$3')
    }

    static String getSyncDesc(SyncConfig syncConfig) {
        "${getLocation(syncConfig.source)} -> ${getLocation(syncConfig.target)}"
    }

    static configureDatabase(SyncConfig syncConfig, grailsApplication) {
        // conform any dbTable specified in the form
        def dbTable = syncConfig.options.dbTable = syncConfig.options.dbTable?.replaceAll(/[^_0-9a-zA-Z]/, '_')
        // if no dbTable or dbFile was specified, create a unique table name
        if (!dbTable && !syncConfig.options.dbFile) {
            dbTable = "sync_${new Date().format(ArchiveEntry.idFormat)}"
            syncConfig.properties.generatedTableName = dbTable
        }
        // if no dbFile or dbConnectString, use the dbConnectString in the UI app config
        if (!syncConfig.options.dbFile && !syncConfig.options.dbConnectString) {
            syncConfig.options.dbConnectString = grailsApplication.config.sync.dbConnectString
            if (!syncConfig.options.dbConnectString) throw new RuntimeException('dbConnectString.missing')
        }
    }

    static generatedTable(SyncConfig syncConfig) {
        return syncConfig.properties.generatedTableName
    }

    static resetGeneratedTable(SyncConfig syncConfig) {
        if (generatedTable(syncConfig)) {
            syncConfig.options.dbConnectString = null
            syncConfig.options.dbFile = null
            syncConfig.options.dbTable = null
            syncConfig.properties.remove('generatedTableName')
        }
    }

    static bindSyncConfig(DataBinder binder, SyncConfig target, GrailsParameterMap paramMap) {
        if (!target) target = new SyncConfig()
        if (paramMap) {
            binder.bindData(target, paramMap)
            binder.bindData(target.options, paramMap.options)
            ConfigUtil.validate(target.options)
            // bind dynamic source
            target.source = bindNewPlugin(binder, paramMap.source)
            if (!target.source) throw new BindException(target, 'source')
            // bind dynamic target
            target.target = bindNewPlugin(binder, paramMap.target)
            if (!target.target) throw new BindException(target, 'target')
            // bind dynamic filters
            for (def i = 0; paramMap["filters[${i}]"]; i++) {
                def filter = bindNewPlugin(binder, paramMap["filters[${i}]"])
                if (target.filters == null) target.filters = []
                target.filters.add(filter)
            }
        }
        target
    }

    static bindNewPlugin(DataBinder binder, GrailsParameterMap paramMap) {
        if (paramMap) {
            if (!paramMap['pluginClass']) throw new ConfigurationException("No plugin class selected")
            def target = Class.forName(paramMap['pluginClass']).newInstance()
            binder.bindData(target, paramMap)
            ConfigUtil.validate(target)
            target
        }
    }

    static coerceFilterParams(GrailsParameterMap paramMap) {
        if (paramMap) {
            // need to coerce filter params into a list for the taglib
            paramMap.filters = paramMap.findAll { it.key.matches(/filters\[[0-9]*\]/) }.collect { k, v -> v }
        }
    }
}
