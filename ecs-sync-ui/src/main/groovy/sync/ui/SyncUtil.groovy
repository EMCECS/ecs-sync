/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package sync.ui

import com.emc.ecs.sync.config.ConfigUtil
import com.emc.ecs.sync.config.ConfigurationException
import com.emc.ecs.sync.config.SyncConfig
import com.emc.ecs.sync.rest.JobControlStatus
import com.emc.ecs.sync.rest.SyncProgress
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

    static Double calculateProgress(SyncProgress progress) {
        if (progress.status == JobControlStatus.Complete) {
            1.toDouble()
        } else {
            // when byte *and* object estimates are available, progress is based on a weighted average of the two
            // percentages with the lesser value counted twice i.e.:
            // ( 2 * min(bytePercent, objectPercent) + max(bytePercent, objectPercent) ) / 3
            double byteRatio = 0, objectRatio = 0, completionRatio = 0
            long totalBytes = progress.totalBytesExpected.toLong() - progress.bytesSkipped.toLong()
            long totalObjects = progress.totalObjectsExpected.toLong() - progress.objectsSkipped.toLong()
            if (progress != null && progress.runtimeMs.toLong() > 0) {
                if (totalBytes > 0) {
                    byteRatio = (double) progress.bytesComplete.toLong() / totalBytes
                    completionRatio = byteRatio
                }
                if (totalObjects > 0) {
                    objectRatio = (double) progress.objectsComplete.toLong() / totalObjects
                    completionRatio = objectRatio
                }
                if (byteRatio > 0 && objectRatio > 0)
                    completionRatio = (2 * Math.min(byteRatio, objectRatio) + Math.max(byteRatio, objectRatio)) / 3
            }
            completionRatio
        }
    }

    static conformDbTable(String name) {
        return name?.replaceAll(/[^_0-9a-zA-Z]/,'_')
    }

    static configureDatabase(SyncConfig syncConfig, grailsApplication) {
        // conform any dbTable specified in the form
        String dbTable = syncConfig.options.dbTable = conformDbTable(syncConfig.options.dbTable)
        // if no dbTable or dbFile was specified, create a unique table name
        if (!dbTable && !syncConfig.options.dbFile) {
            dbTable = "sync_${new Date().format(SyncHistoryEntry.idFormat)}"
            syncConfig.properties.generatedTableName = dbTable
        }
        // if no dbFile or dbConnectString, use the dbConnectString in the UI app config
        if (!syncConfig.options.dbFile && !syncConfig.options.dbConnectString) {
            syncConfig.options.dbConnectString = grailsApplication.config.sync.dbConnectString
            if (!syncConfig.options.dbConnectString) throw new RuntimeException('dbConnectString.missing')
            if (grailsApplication.config.sync.dbEncPassword) // also set the encrypted password if specified
                syncConfig.options.dbEncPassword = grailsApplication.config.sync.dbEncPassword
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
