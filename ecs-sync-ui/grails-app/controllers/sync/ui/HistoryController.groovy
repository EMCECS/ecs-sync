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

import com.emc.ecs.sync.config.SyncConfig

import javax.xml.bind.Marshaller

class HistoryController implements ConfigAccessor {
    def syncHttpMessageConverter

    def index() {
        def entries = HistoryEntry.list(configService)
        [historyEntries: entries]
    }

    def delete() {
        new HistoryEntry([id: params.entryId]).allKeys.each { configService.deleteConfigObject(it) }
        redirect action: 'index'
    }

    def getSyncXml() {
        def historyEntry = new HistoryEntry(id: params.entryId, configService: configService)

        // reset any auto-generated DB table
        SyncUtil.resetGeneratedTable(historyEntry.syncResult.config)

        // marshall the config to XML
        // have to do it this way as Grails does not support automatic rendering of JAXB (wha??)
        def xmlWriter = new StringWriter()
        def marshaller = syncHttpMessageConverter.getContext(SyncConfig).createMarshaller()
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE)
        marshaller.marshal(historyEntry.syncResult.config, xmlWriter)

        // remove NS prefix (easier to read)
        def xmlString = xmlWriter.toString().replaceFirst(/ xmlns:ns2=/, ' xmlns=')
                .replaceAll(/(<\/?)ns2:/, '$1')

        def filename = "${historyEntry.id}.xml"

        response.setHeader('Content-Disposition', "attachment; filename=${filename}")
        render text: xmlString, contentType: 'text/xml'
    }
}
