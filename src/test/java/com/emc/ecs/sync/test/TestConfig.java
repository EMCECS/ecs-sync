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
package com.emc.ecs.sync.test;

import org.junit.Assume;

import java.io.*;
import java.util.Properties;

/**
 * Utility functions to configure EcsSync through an ecs-sync.properties file located on either
 * the classpath or in the user's home directory.
 */
public class TestConfig {
    public static final String PROPERTIES_FILE = "ecs-sync.properties";

    public static final String PROP_S3_ACCESS_KEY_ID = "s3.access_key_id";
    public static final String PROP_S3_SECRET_KEY = "s3.secret_key";
    public static final String PROP_S3_ENDPOINT = "s3.endpoint";
    public static final String PROP_S3_VHOST = "s3.vhost";

    public static final String PROP_ATMOS_UID = "atmos.uid";
    public static final String PROP_ATMOS_SECRET = "atmos.secret_key";
    public static final String PROP_ATMOS_ENDPOINTS = "atmos.endpoints";
    public static final String PROP_ATMOS_IS_ECS = "atmos.is_ecs";

    public static final String PROP_CAS_CONNECT_STRING = "cas.connect";
    public static final String PROP_CENTERA_CONNECT_STRING = "centera.connect";
    public static final String PROP_CAS_RETENTION_CLASS = "cas.retention_class";
    public static final String PROP_CAS_RETENTION_PERIOD = "cas.retention_period";
    public static final String PROP_CAS_RETENTION_PERIOD_EBR = "cas.retention_period_ebr";

    public static final String PROP_NFS_EXPORT = "nfs.export";

    public static final String PROP_HTTP_PROXY_URI = "http.proxyUri";

    public static final String PROP_MYSQL_CONNECT_STRING = "mysql.connect_string";
    public static final String PROP_MYSQL_ENC_PASSWORD = "mysql.enc_password";

    public static final String PROP_AZURE_BLOB_CONNECT_STRING = "azure.blob.connect";
    public static final String PROP_AZURE_BLOB_CONTAINER_NAME = "azure.blob.container";
    /**
     * Locates and loads the properties file for the test configuration.  This file can
     * reside in one of two places: somewhere in the CLASSPATH or in the user's home
     * directory.
     *
     * @return the contents of the properties file as a {@link Properties} object.
     * @throws FileNotFoundException if the file was not found
     * @throws IOException           if there was an error reading the file.
     */
    public static Properties getProperties() throws IOException {
        InputStream in = TestConfig.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE);
        if (in == null) {
            // Check in home directory
            File homeProps = new File(System.getProperty("user.home") + File.separator +
                    PROPERTIES_FILE);
            if (homeProps.exists()) {
                in = new FileInputStream(homeProps);
            }
        }

        if (in == null) {
            Assume.assumeTrue(PROPERTIES_FILE + " missing (look in src/test/resources for template)", false);
            throw new RuntimeException(PROPERTIES_FILE + " missing (look in src/test/resources for template)");
        }

        Properties props = new Properties();
        props.load(in);
        in.close();

        return props;
    }

    /**
     * Utility method that gets a key from a Properties object and throws a
     * RuntimeException if the key does not exist or is not set.
     */
    public static String getPropertyNotEmpty(Properties p, String key) {
        String value = p.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new RuntimeException(String.format("The property %s is required", key));
        }
        return value;
    }
}
