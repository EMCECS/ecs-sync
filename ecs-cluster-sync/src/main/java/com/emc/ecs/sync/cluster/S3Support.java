/*
 * Copyright (c) 2024 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.cluster;

import com.emc.object.Protocol;
import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.bean.VersioningConfiguration;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.rest.smart.ecs.Vdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Small helpers for the S3 data path. Used to determine and reconcile bucket versioning state, which the management
 * API does not reliably report across ECS versions.
 */
final class S3Support {
    private static final Logger log = LoggerFactory.getLogger(S3Support.class);

    private S3Support() {
    }

    static S3Client createClient(ClusterEndpoint endpoint, String namespace, String accessKey, String secretKey) {
        String[] hosts = endpoint.effectiveDataEndpoints();
        S3Config config = new S3Config(
                Protocol.valueOf(endpoint.getDataProtocol().toString().toUpperCase()), new Vdc(hosts));
        config.setPort(endpoint.getDataPort());
        config.setSmartClient(endpoint.isSmartClientEnabled());
        if (namespace != null) config.setNamespace(namespace);
        config.withIdentity(accessKey).withSecretKey(secretKey);
        return new S3JerseyClient(config);
    }

    /**
     * The hosts to hand to {@link com.emc.ecs.sync.config.storage.EcsS3Config}. A single host is passed as the plain
     * host; multiple hosts are passed as a single comma-separated VDC, which is the format the ECS S3 plugin parses.
     */
    static void applyEndpoint(ClusterEndpoint endpoint, com.emc.ecs.sync.config.storage.EcsS3Config s3Config,
                              String namespace) {
        List<String> hosts = Arrays.asList(endpoint.effectiveDataEndpoints());
        s3Config.setNamespace(namespace);
        s3Config.setProtocol(endpoint.getDataProtocol());
        s3Config.setPort(endpoint.getDataPort());
        s3Config.setSmartClientEnabled(endpoint.isSmartClientEnabled());
        if (hosts.size() == 1) {
            s3Config.setHost(hosts.get(0));
        } else {
            s3Config.setVdcs(new String[]{String.join(",", hosts)});
        }
    }

    static boolean isVersioningEnabled(S3Client s3, String bucket) {
        VersioningConfiguration configuration = s3.getBucketVersioning(bucket);
        return configuration != null && configuration.getStatus() == VersioningConfiguration.Status.Enabled;
    }

    static void enableVersioning(S3Client s3, String bucket) {
        s3.setBucketVersioning(bucket,
                new VersioningConfiguration().withStatus(VersioningConfiguration.Status.Enabled));
        log.info("enabled versioning on target bucket {}", bucket);
    }
}
