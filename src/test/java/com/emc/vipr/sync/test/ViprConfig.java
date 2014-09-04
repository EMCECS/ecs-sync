/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.test;

import java.io.*;
import java.util.Properties;

/**
 * Utility functions to configure ViPR through a vipr.properties file located on either
 * the classpath or in the user's home directory.
 */
public class ViprConfig {
    public static final String VIPR_PROPERTIES_FILE = "vipr.properties";

    public static final String PROP_S3_ACCESS_KEY_ID = "vipr.s3.access_key_id";
    public static final String PROP_S3_SECRET_KEY = "vipr.s3.secret_key";
    public static final String PROP_S3_ENDPOINT = "vipr.s3.endpoint";
    public static final String PROP_S3_ENDPOINTS = "vipr.s3.endpoints";
    public static final String PROP_NAMESPACE = "vipr.namespace";
    public static final String PROP_PUBLIC_KEY = "vipr.encryption.publickey";
    public static final String PROP_PRIVATE_KEY = "vipr.encryption.privatekey";
    public static final String PROP_PROXY_HOST = "vipr.proxy.host";
    public static final String PROP_PROXY_PORT = "vipr.proxy.port";
    public static final String PROP_FILE_ACCESS_TESTS_ENABLED = "vipr.file.access.tests.enabled";

    public static final String PROP_ATMOS_UID = "vipr.atmos.uid";
    public static final String PROP_ATMOS_SECRET = "vipr.atmos.secret_key";
    public static final String PROP_ATMOS_ENDPOINTS = "vipr.atmos.endpoints";
    public static final String PROP_ATMOS_IS_VIPR = "vipr.atmos.is_vipr";

    public static final String PROP_CAS_CONNECT_STRING = "vipr.cas.connect";

    public static final String PROP_ACDP_ADMIN_ENDPOINT = "acdp.admin.endpoint";
    public static final String PROP_ACDP_ADMIN_USERNAME =  "acdp.admin.username";
    public static final String PROP_ACDP_ADMIN_PASSWORD = "acdp.admin.password";
    public static final String PROP_ACDP_MGMT_ENDPOINT = "acdp.mgmt.endpoint";
    public static final String PROP_ACDP_MGMT_USERNAME =  "acdp.mgmt.username";
    public static final String PROP_ACDP_MGMT_PASSWORD = "acdp.mgmt.password";

    public static final String PROP_ATMOS_SYSMGMT_PROTO = "atmos.sysmgmt.proto";
    public static final String PROP_ATMOS_SYSMGMT_HOST = "atmos.sysmgmt.host";
    public static final String PROP_ATMOS_SYSMGMT_PORT = "atmos.sysmgmt.port";
    public static final String PROP_ATMOS_SYSMGMT_USER = "atmos.sysmgmt.username";
    public static final String PROP_ATMOS_SYSMGMT_PASS = "atmos.sysmgmt.password";

    /**
     * Locates and loads the properties file for the test configuration.  This file can
     * reside in one of two places: somewhere in the CLASSPATH or in the user's home
     * directory.
     * @return the contents of the properties file as a {@link Properties} object.
     * @throws FileNotFoundException if the file was not found
     * @throws IOException if there was an error reading the file.
     */
    public static Properties getProperties() throws IOException {
        InputStream in = ViprConfig.class.getClassLoader().getResourceAsStream(VIPR_PROPERTIES_FILE);
        if(in == null) {
            // Check in home directory
            File homeProps = new File(System.getProperty("user.home") + File.separator +
                    VIPR_PROPERTIES_FILE);
            if(homeProps.exists()) {
                in = new FileInputStream(homeProps);
            } else {
                throw new FileNotFoundException(VIPR_PROPERTIES_FILE);
            }
        }

        Properties props = new Properties();
        props.load(in);
        in.close();

        return props;
    }

    public static String getProxyUri(Properties p) {
        String host = p.getProperty(PROP_PROXY_HOST);
        String port = p.getProperty(PROP_PROXY_PORT, "80");

        if(host == null) {
            // disabled
            return null;
        }

        String scheme = "http";
        if(port.endsWith("43")) {
            scheme = "https";
        }

        return String.format("%s://%s:%s/", scheme, host, port);
    }


    /**
     * Utility method that gets a key from a Properties object and throws a
     * RuntimeException if the key does not exist or is not set.
     */
    public static String getPropertyNotEmpty(Properties p, String key) {
        String value = p.getProperty(key);
        if(value == null || value.isEmpty()) {
            throw new RuntimeException(String.format("The property %s is required", key));
        }
        return value;
    }


}

