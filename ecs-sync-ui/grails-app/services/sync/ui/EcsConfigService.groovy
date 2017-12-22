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

import com.emc.object.Protocol
import com.emc.object.s3.S3Client
import com.emc.object.s3.S3Config
import com.emc.object.s3.S3Exception
import com.emc.object.s3.jersey.S3JerseyClient
import com.emc.object.s3.request.ListObjectsRequest
import grails.transaction.Transactional
import org.springframework.beans.BeanUtils

@Transactional(readOnly = true)
class EcsConfigService extends ConfigService {
    static clientMap = [:]

    List<String> listConfigObjects(String prefix) {
        def uiConfig = getConfig()
        def request = new ListObjectsRequest(uiConfig.configBucket).withPrefix(prefix).withDelimiter('/')
        getEcsClient(uiConfig).listObjects(request).objects.collect { it.key }
    }

    boolean configObjectExists(String key) {
        if (!key) return false
        try {
            def uiConfig = getConfig()
            getEcsClient(uiConfig).getObjectMetadata(uiConfig.configBucket, key)
            return true
        } catch (S3Exception e) {
            if (e.httpCode == 404) return false
            else throw e
        }
    }

    def <T> T readConfigObject(String key, Class<T> resultType) {
        def uiConfig = getConfig()
        getEcsClient(uiConfig).readObject(uiConfig.configBucket, key, resultType)
    }

    void writeConfigObject(String key, content, String contentType) {
        def uiConfig = getConfig()
        getEcsClient(uiConfig).putObject(uiConfig.configBucket, key, content, contentType)
    }

    void deleteConfigObject(String key) {
        def uiConfig = getConfig()
        if (key) getEcsClient(uiConfig).deleteObject(uiConfig.configBucket, key)
    }

    URI configObjectQuickLink(String key) {
        def uiConfig = getConfig()
        getEcsClient(uiConfig).getPresignedUrl(uiConfig.configBucket, key, 4.hours.from.now).toURI()
    }

    void writeConfig(UiConfig uiConfig) {
        def ecs = getEcsClient(uiConfig)
        if (!ecs.bucketExists(uiConfig.configBucket)) ecs.createBucket(uiConfig.configBucket)
        ecs.putObject(uiConfig.configBucket, 'ui-config.xml', uiConfig, 'application/xml')
    }

    void readConfig(UiConfig uiConfig) {
        def ecs = getEcsClient(uiConfig)
        BeanUtils.copyProperties(ecs.readObject(uiConfig.configBucket, 'ui-config.xml', UiConfig.class), uiConfig, 'id')
    }

    private static S3Client getEcsClient(UiConfig uiConfig) {
        def key = "${uiConfig.accessKey}:${uiConfig.secretKey}@${uiConfig.hosts}"
        def client = clientMap[key]
        if (!client) {
            client = new S3JerseyClient(new S3Config(Protocol.valueOf(uiConfig.protocol.toUpperCase()), uiConfig.hosts.split(','))
                    .withPort(uiConfig.port).withIdentity(uiConfig.accessKey).withSecretKey(uiConfig.secretKey))
            clientMap[key] = client
        }
        return client as S3Client
    }
}
