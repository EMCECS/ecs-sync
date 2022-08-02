/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config;

import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Option;
import com.emc.ecs.sync.config.annotation.StorageConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

public class XmlGeneratorTest {
    @Test
    public void testBasic() throws Exception {
        String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
                "<syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\">\n" +
                "    <options>\n" +
                "        <!-- Metadata is synced by default -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <syncMetadata>true</syncMetadata>\n" +
                "        <!-- Sync retention/expiration information when syncing objects (in supported plugins). The target plugin will *attempt* to replicate retention/expiration for each object. Works only on plugins that support retention/expiration. If the target is an Atmos cloud, the target policy must enable retention/expiration immediately for this to work -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <syncRetentionExpiration>false</syncRetentionExpiration>\n" +
                "        <!-- Sync ACL information when syncing objects (in supported plugins) -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <syncAcl>false</syncAcl>\n" +
                "        <!-- Object data is synced by default -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <syncData>true</syncData>\n" +
                "        <!-- By default, the source plugin will query the source storage to crawl and estimate the total amount of data to be transferred. Use this option to disable estimation (i.e. for performance improvement) -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <estimationEnabled>true</estimationEnabled>\n" +
                "        <!-- The list of source objects to sync. Unless sourceListRawValues is enabled, this should be in CSV format, with one object per line, where the absolute identifier (full path or key) is the first value in each line. This entire line is available to each plugin as a raw string -->\n" +
                "        <!-- String[] - Repeat for multiple values -->\n" +
                "        <sourceList>sourceList</sourceList>\n" +
                "        <!-- Path to a file that supplies the list of source objects to sync. Unless sourceListRawValues is enabled, this file should be in CSV format, with one object per line, where the absolute identifier (full path or key) is the first value in each line. This entire line is available to each plugin as a raw string -->\n" +
                "        <!-- String -->\n" +
                "        <sourceListFile>sourceListFile</sourceListFile>\n" +
                "        <!-- Whether to treat the lines in the sourceList or sourceListFile as raw object identifier values (do not do any CSV parsing and do not remove comments, escapes, or trim white space). Default is false -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <sourceListRawValues>false</sourceListRawValues>\n" +
                "        <!-- Hierarchical storage will sync recursively by default -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <recursive>true</recursive>\n" +
                "        <!-- If syncing ACL information when syncing objects, ignore any invalid entries (i.e. permissions or identities that don't exist in the target system) -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <ignoreInvalidAcls>false</ignoreInvalidAcls>\n" +
                "        <!-- Force the write of each object, regardless of its state in the target storage -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <forceSync>false</forceSync>\n" +
                "        <!-- After a successful object transfer, the object will be read back from the target system and its MD5 checksum will be compared with that of the source object (generated during transfer). This only compares object data (metadata is not compared) and does not include directories -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <verify>false</verify>\n" +
                "        <!-- Similar to - -verify except that the object transfer is skipped and only read operations are performed (no data is written) -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <verifyOnly>false</verifyOnly>\n" +
                "        <!-- Supported source plugins will delete each source object once it is successfully synced (does not include directories). Use this option with care! Be sure log levels are appropriate to capture transferred (source deleted) objects -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <deleteSource>false</deleteSource>\n" +
                "        <!-- Sets the buffer size (in bytes) to use when streaming data from the source to the target (supported plugins only). Defaults to 128K -->\n" +
                "        <!-- int - Default: 131072 -->\n" +
                "        <bufferSize>131072</bufferSize>\n" +
                "        <!-- Specifies the number of objects to sync simultaneously. Default is 16 -->\n" +
                "        <!-- int - Default: 16 -->\n" +
                "        <threadCount>16</threadCount>\n" +
                "        <!-- Specifies how many times each object should be retried after an error. Default is 2 retries (total of 3 attempts) -->\n" +
                "        <!-- int - Default: 2 -->\n" +
                "        <retryAttempts>2</retryAttempts>\n" +
                "        <!-- Enables performance monitoring for reads and writes on any plugin that supports it. This information is available via the REST service during a sync -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <monitorPerformance>true</monitorPerformance>\n" +
                "        <!-- Enables operation timings on all plug-ins that support it -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <timingsEnabled>false</timingsEnabled>\n" +
                "        <!-- Sets the window for timing statistics. Every {timingWindow} objects that are synced, timing statistics are logged and reset. Default is 10,000 objects -->\n" +
                "        <!-- int - Default: 1000 -->\n" +
                "        <timingWindow>1000</timingWindow>\n" +
                "        <!-- Tracks all failed objects and displays a summary of failures when finished -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <rememberFailed>false</rememberFailed>\n" +
                "        <!-- Enables the Sqlite database engine and specifies the file to hold the status database. A database will make repeat runs and incrementals more efficient. With this database type, you can use the sqlite3 client to interrogate the details of all objects in the sync -->\n" +
                "        <!-- String -->\n" +
                "        <dbFile>dbFile</dbFile>\n" +
                "        <!-- Enables the MySQL database engine and specifies the JDBC connect string to connect to the database (i.e. \"jdbc:mysql://localhost:3306/ecs_sync?user=foo&password=bar\"). A database will make repeat runs and incrementals more efficient. With this database type, you can use the mysql client to interrogate the details of all objects in the sync. Note that in the UI, this option is the default and is automatically populated by the server (you don't need a value here) -->\n" +
                "        <!-- String -->\n" +
                "        <dbConnectString>dbConnectString</dbConnectString>\n" +
                "        <!-- Specifies the encrypted password for the MySQL database -->\n" +
                "        <!-- String -->\n" +
                "        <dbEncPassword>dbEncPassword</dbEncPassword>\n" +
                "        <!-- Specifies the DB table name to use. When using MySQL or the UI, be sure to provide a unique table name or risk corrupting a previously used table. Default table is \"objects\" except in the UI, where a unique name is generated for each job. In the UI, you should specify a table name to ensure the table persists after the job is archived -->\n" +
                "        <!-- String -->\n" +
                "        <dbTable>dbTable</dbTable>\n" +
                "        <!-- Specifies whether the DB should included enhanced details, like source/target MD5 checksum, retention durations, etc. Note this will cause the DB to consume more storage and may add some latency to each copy operation -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <dbEnhancedDetailsEnabled>false</dbEnhancedDetailsEnabled>\n" +
                "        <!-- When available, use the checksum in the metadata of the object (e.g. S3 ETag) during verification, instead of reading back the object data. This may improve efficiency by avoiding a full read of the object data to verify source and target. However, you must fully trust the checksum provided by both source and target storage -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <useMetadataChecksumForVerification>false</useMetadataChecksumForVerification>\n" +
                "        <!-- Specify the max speed in bytes/s to throttle the traffic bandwidth. Default is 0 (no throttle). Note that if verify is enabled, the target storage will have reads and writes using the same throttle limit, so make sure your bandwidth is sufficient to support full duplex traffic -->\n" +
                "        <!-- int - Default: 0 -->\n" +
                "        <bandwidthLimit>0</bandwidthLimit>\n" +
                "        <!-- Specify the max TPS throughput limit in objects/s. Default is 0 (no throttle) -->\n" +
                "        <!-- int - Default: 0 -->\n" +
                "        <throughputLimit>0</throughputLimit>\n" +
                "    </options>\n" +
                "    <source>\n" +
                "        <!-- Xml Generator Storage documentation -->\n" +
                "        <xGSConfig>\n" +
                "            <!-- multiple values array -->\n" +
                "            <!-- String[] - Repeat for multiple values -->\n" +
                "            <array>value</array>\n" +
                "            <!-- really big number -->\n" +
                "            <!-- Long -->\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <!-- yes or no -->\n" +
                "            <!-- boolean - Default: true -->\n" +
                "            <bool>true</bool>\n" +
                "            <!-- number description -->\n" +
                "            <!-- int - Default: 4 -->\n" +
                "            <number>4</number>\n" +
                "            <!-- restricted string values -->\n" +
                "            <!-- String - Values: [value1, value2, value3] -->\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <!-- basic string -->\n" +
                "            <!-- String - Required -->\n" +
                "            <string>string</string>\n" +
                "            <!-- restricted by enum -->\n" +
                "            <!-- XGType - Values: [foo, bar, baz] - Default: bar -->\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </source>\n" +
                "    <target>\n" +
                "        <!-- Xml Generator Storage documentation -->\n" +
                "        <xGSConfig>\n" +
                "            <!-- multiple values array -->\n" +
                "            <!-- String[] - Repeat for multiple values -->\n" +
                "            <array>value</array>\n" +
                "            <!-- really big number -->\n" +
                "            <!-- Long -->\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <!-- yes or no -->\n" +
                "            <!-- boolean - Default: true -->\n" +
                "            <bool>true</bool>\n" +
                "            <!-- number description -->\n" +
                "            <!-- int - Default: 4 -->\n" +
                "            <number>4</number>\n" +
                "            <!-- restricted string values -->\n" +
                "            <!-- String - Values: [value1, value2, value3] -->\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <!-- basic string -->\n" +
                "            <!-- String - Required -->\n" +
                "            <string>string</string>\n" +
                "            <!-- restricted by enum -->\n" +
                "            <!-- XGType - Values: [foo, bar, baz] - Default: bar -->\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </target>\n" +
                "</syncConfig>\n";

        String generatedXml = XmlGenerator.generateXml(true, true, "xgs:", "xgs:");

        // remove carriage returns on Windows
        generatedXml = generatedXml.replaceAll("\r", "");

        Assertions.assertEquals(expectedXml, generatedXml);
    }

    @Test
    public void testFilters() throws Exception {
        String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
                "<syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\">\n" +
                "    <options>\n" +
                "        <!-- Metadata is synced by default -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <syncMetadata>true</syncMetadata>\n" +
                "        <!-- Sync retention/expiration information when syncing objects (in supported plugins). The target plugin will *attempt* to replicate retention/expiration for each object. Works only on plugins that support retention/expiration. If the target is an Atmos cloud, the target policy must enable retention/expiration immediately for this to work -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <syncRetentionExpiration>false</syncRetentionExpiration>\n" +
                "        <!-- Sync ACL information when syncing objects (in supported plugins) -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <syncAcl>false</syncAcl>\n" +
                "        <!-- Object data is synced by default -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <syncData>true</syncData>\n" +
                "        <!-- By default, the source plugin will query the source storage to crawl and estimate the total amount of data to be transferred. Use this option to disable estimation (i.e. for performance improvement) -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <estimationEnabled>true</estimationEnabled>\n" +
                "        <!-- The list of source objects to sync. Unless sourceListRawValues is enabled, this should be in CSV format, with one object per line, where the absolute identifier (full path or key) is the first value in each line. This entire line is available to each plugin as a raw string -->\n" +
                "        <!-- String[] - Repeat for multiple values -->\n" +
                "        <sourceList>sourceList</sourceList>\n" +
                "        <!-- Path to a file that supplies the list of source objects to sync. Unless sourceListRawValues is enabled, this file should be in CSV format, with one object per line, where the absolute identifier (full path or key) is the first value in each line. This entire line is available to each plugin as a raw string -->\n" +
                "        <!-- String -->\n" +
                "        <sourceListFile>sourceListFile</sourceListFile>\n" +
                "        <!-- Whether to treat the lines in the sourceList or sourceListFile as raw object identifier values (do not do any CSV parsing and do not remove comments, escapes, or trim white space). Default is false -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <sourceListRawValues>false</sourceListRawValues>\n" +
                "        <!-- Hierarchical storage will sync recursively by default -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <recursive>true</recursive>\n" +
                "        <!-- If syncing ACL information when syncing objects, ignore any invalid entries (i.e. permissions or identities that don't exist in the target system) -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <ignoreInvalidAcls>false</ignoreInvalidAcls>\n" +
                "        <!-- Force the write of each object, regardless of its state in the target storage -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <forceSync>false</forceSync>\n" +
                "        <!-- After a successful object transfer, the object will be read back from the target system and its MD5 checksum will be compared with that of the source object (generated during transfer). This only compares object data (metadata is not compared) and does not include directories -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <verify>false</verify>\n" +
                "        <!-- Similar to - -verify except that the object transfer is skipped and only read operations are performed (no data is written) -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <verifyOnly>false</verifyOnly>\n" +
                "        <!-- Supported source plugins will delete each source object once it is successfully synced (does not include directories). Use this option with care! Be sure log levels are appropriate to capture transferred (source deleted) objects -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <deleteSource>false</deleteSource>\n" +
                "        <!-- Sets the buffer size (in bytes) to use when streaming data from the source to the target (supported plugins only). Defaults to 128K -->\n" +
                "        <!-- int - Default: 131072 -->\n" +
                "        <bufferSize>131072</bufferSize>\n" +
                "        <!-- Specifies the number of objects to sync simultaneously. Default is 16 -->\n" +
                "        <!-- int - Default: 16 -->\n" +
                "        <threadCount>16</threadCount>\n" +
                "        <!-- Specifies how many times each object should be retried after an error. Default is 2 retries (total of 3 attempts) -->\n" +
                "        <!-- int - Default: 2 -->\n" +
                "        <retryAttempts>2</retryAttempts>\n" +
                "        <!-- Enables performance monitoring for reads and writes on any plugin that supports it. This information is available via the REST service during a sync -->\n" +
                "        <!-- boolean - Default: true -->\n" +
                "        <monitorPerformance>true</monitorPerformance>\n" +
                "        <!-- Enables operation timings on all plug-ins that support it -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <timingsEnabled>false</timingsEnabled>\n" +
                "        <!-- Sets the window for timing statistics. Every {timingWindow} objects that are synced, timing statistics are logged and reset. Default is 10,000 objects -->\n" +
                "        <!-- int - Default: 1000 -->\n" +
                "        <timingWindow>1000</timingWindow>\n" +
                "        <!-- Tracks all failed objects and displays a summary of failures when finished -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <rememberFailed>false</rememberFailed>\n" +
                "        <!-- Enables the Sqlite database engine and specifies the file to hold the status database. A database will make repeat runs and incrementals more efficient. With this database type, you can use the sqlite3 client to interrogate the details of all objects in the sync -->\n" +
                "        <!-- String -->\n" +
                "        <dbFile>dbFile</dbFile>\n" +
                "        <!-- Enables the MySQL database engine and specifies the JDBC connect string to connect to the database (i.e. \"jdbc:mysql://localhost:3306/ecs_sync?user=foo&password=bar\"). A database will make repeat runs and incrementals more efficient. With this database type, you can use the mysql client to interrogate the details of all objects in the sync. Note that in the UI, this option is the default and is automatically populated by the server (you don't need a value here) -->\n" +
                "        <!-- String -->\n" +
                "        <dbConnectString>dbConnectString</dbConnectString>\n" +
                "        <!-- Specifies the encrypted password for the MySQL database -->\n" +
                "        <!-- String -->\n" +
                "        <dbEncPassword>dbEncPassword</dbEncPassword>\n" +
                "        <!-- Specifies the DB table name to use. When using MySQL or the UI, be sure to provide a unique table name or risk corrupting a previously used table. Default table is \"objects\" except in the UI, where a unique name is generated for each job. In the UI, you should specify a table name to ensure the table persists after the job is archived -->\n" +
                "        <!-- String -->\n" +
                "        <dbTable>dbTable</dbTable>\n" +
                "        <!-- Specifies whether the DB should included enhanced details, like source/target MD5 checksum, retention durations, etc. Note this will cause the DB to consume more storage and may add some latency to each copy operation -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <dbEnhancedDetailsEnabled>false</dbEnhancedDetailsEnabled>\n" +
                "        <!-- When available, use the checksum in the metadata of the object (e.g. S3 ETag) during verification, instead of reading back the object data. This may improve efficiency by avoiding a full read of the object data to verify source and target. However, you must fully trust the checksum provided by both source and target storage -->\n" +
                "        <!-- boolean - Default: false -->\n" +
                "        <useMetadataChecksumForVerification>false</useMetadataChecksumForVerification>\n" +
                "        <!-- Specify the max speed in bytes/s to throttle the traffic bandwidth. Default is 0 (no throttle). Note that if verify is enabled, the target storage will have reads and writes using the same throttle limit, so make sure your bandwidth is sufficient to support full duplex traffic -->\n" +
                "        <!-- int - Default: 0 -->\n" +
                "        <bandwidthLimit>0</bandwidthLimit>\n" +
                "        <!-- Specify the max TPS throughput limit in objects/s. Default is 0 (no throttle) -->\n" +
                "        <!-- int - Default: 0 -->\n" +
                "        <throughputLimit>0</throughputLimit>\n" +
                "    </options>\n" +
                "    <source>\n" +
                "        <!-- Xml Generator Storage documentation -->\n" +
                "        <xGSConfig>\n" +
                "            <!-- multiple values array -->\n" +
                "            <!-- String[] - Repeat for multiple values -->\n" +
                "            <array>value</array>\n" +
                "            <!-- really big number -->\n" +
                "            <!-- Long -->\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <!-- yes or no -->\n" +
                "            <!-- boolean - Default: true -->\n" +
                "            <bool>true</bool>\n" +
                "            <!-- number description -->\n" +
                "            <!-- int - Default: 4 -->\n" +
                "            <number>4</number>\n" +
                "            <!-- restricted string values -->\n" +
                "            <!-- String - Values: [value1, value2, value3] -->\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <!-- basic string -->\n" +
                "            <!-- String - Required -->\n" +
                "            <string>string</string>\n" +
                "            <!-- restricted by enum -->\n" +
                "            <!-- XGType - Values: [foo, bar, baz] - Default: bar -->\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </source>\n" +
                "    <filters>\n" +
                "        <!-- Xml Generator Filter documentation -->\n" +
                "        <xGFConfig>\n" +
                "            <!-- multiple values array -->\n" +
                "            <!-- String[] - Repeat for multiple values -->\n" +
                "            <array>value</array>\n" +
                "            <!-- really big number -->\n" +
                "            <!-- Long -->\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <!-- yes or no -->\n" +
                "            <!-- boolean - Default: true -->\n" +
                "            <bool>true</bool>\n" +
                "            <!-- number description -->\n" +
                "            <!-- int - Default: 4 -->\n" +
                "            <number>4</number>\n" +
                "            <!-- restricted string values -->\n" +
                "            <!-- String - Values: [value1, value2, value3] -->\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <!-- basic string -->\n" +
                "            <!-- String - Required -->\n" +
                "            <string>string</string>\n" +
                "            <!-- restricted by enum -->\n" +
                "            <!-- XGType - Values: [foo, bar, baz] - Default: bar -->\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGFConfig>\n" +
                "        <!-- Xml Generator Filter documentation -->\n" +
                "        <xGFConfig>\n" +
                "            <!-- multiple values array -->\n" +
                "            <!-- String[] - Repeat for multiple values -->\n" +
                "            <array>value</array>\n" +
                "            <!-- really big number -->\n" +
                "            <!-- Long -->\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <!-- yes or no -->\n" +
                "            <!-- boolean - Default: true -->\n" +
                "            <bool>true</bool>\n" +
                "            <!-- number description -->\n" +
                "            <!-- int - Default: 4 -->\n" +
                "            <number>4</number>\n" +
                "            <!-- restricted string values -->\n" +
                "            <!-- String - Values: [value1, value2, value3] -->\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <!-- basic string -->\n" +
                "            <!-- String - Required -->\n" +
                "            <string>string</string>\n" +
                "            <!-- restricted by enum -->\n" +
                "            <!-- XGType - Values: [foo, bar, baz] - Default: bar -->\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGFConfig>\n" +
                "    </filters>\n" +
                "    <target>\n" +
                "        <!-- Xml Generator Storage documentation -->\n" +
                "        <xGSConfig>\n" +
                "            <!-- multiple values array -->\n" +
                "            <!-- String[] - Repeat for multiple values -->\n" +
                "            <array>value</array>\n" +
                "            <!-- really big number -->\n" +
                "            <!-- Long -->\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <!-- yes or no -->\n" +
                "            <!-- boolean - Default: true -->\n" +
                "            <bool>true</bool>\n" +
                "            <!-- number description -->\n" +
                "            <!-- int - Default: 4 -->\n" +
                "            <number>4</number>\n" +
                "            <!-- restricted string values -->\n" +
                "            <!-- String - Values: [value1, value2, value3] -->\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <!-- basic string -->\n" +
                "            <!-- String - Required -->\n" +
                "            <string>string</string>\n" +
                "            <!-- restricted by enum -->\n" +
                "            <!-- XGType - Values: [foo, bar, baz] - Default: bar -->\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </target>\n" +
                "</syncConfig>\n";

        String generatedXml = XmlGenerator.generateXml(true, true, "xgs:", "xgs:", "xgf", "xgf");

        // remove carriage returns on Windows
        generatedXml = generatedXml.replaceAll("\r", "");

        Assertions.assertEquals(expectedXml, generatedXml);
    }

    @Test
    public void testNoComments() throws Exception {
        String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
                "<syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\">\n" +
                "    <options>\n" +
                "        <syncMetadata>true</syncMetadata>\n" +
                "        <syncRetentionExpiration>false</syncRetentionExpiration>\n" +
                "        <syncAcl>false</syncAcl>\n" +
                "        <syncData>true</syncData>\n" +
                "        <estimationEnabled>true</estimationEnabled>\n" +
                "        <sourceList>sourceList</sourceList>\n" +
                "        <sourceListFile>sourceListFile</sourceListFile>\n" +
                "        <sourceListRawValues>false</sourceListRawValues>\n" +
                "        <recursive>true</recursive>\n" +
                "        <ignoreInvalidAcls>false</ignoreInvalidAcls>\n" +
                "        <forceSync>false</forceSync>\n" +
                "        <verify>false</verify>\n" +
                "        <verifyOnly>false</verifyOnly>\n" +
                "        <deleteSource>false</deleteSource>\n" +
                "        <bufferSize>131072</bufferSize>\n" +
                "        <threadCount>16</threadCount>\n" +
                "        <retryAttempts>2</retryAttempts>\n" +
                "        <monitorPerformance>true</monitorPerformance>\n" +
                "        <timingsEnabled>false</timingsEnabled>\n" +
                "        <timingWindow>1000</timingWindow>\n" +
                "        <rememberFailed>false</rememberFailed>\n" +
                "        <dbFile>dbFile</dbFile>\n" +
                "        <dbConnectString>dbConnectString</dbConnectString>\n" +
                "        <dbEncPassword>dbEncPassword</dbEncPassword>\n" +
                "        <dbTable>dbTable</dbTable>\n" +
                "        <dbEnhancedDetailsEnabled>false</dbEnhancedDetailsEnabled>\n" +
                "        <useMetadataChecksumForVerification>false</useMetadataChecksumForVerification>\n" +
                "        <bandwidthLimit>0</bandwidthLimit>\n" +
                "        <throughputLimit>0</throughputLimit>\n" +
                "    </options>\n" +
                "    <source>\n" +
                "        <xGSConfig>\n" +
                "            <array>value</array>\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <bool>true</bool>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </source>\n" +
                "    <filters>\n" +
                "        <xGFConfig>\n" +
                "            <array>value</array>\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <bool>true</bool>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGFConfig>\n" +
                "        <xGFConfig>\n" +
                "            <array>value</array>\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <bool>true</bool>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGFConfig>\n" +
                "    </filters>\n" +
                "    <target>\n" +
                "        <xGSConfig>\n" +
                "            <array>value</array>\n" +
                "            <bigNumber>bigNumber</bigNumber>\n" +
                "            <bool>true</bool>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </target>\n" +
                "</syncConfig>\n";

        String generatedXml = XmlGenerator.generateXml(false, true, "xgs:", "xgs:", "xgf", "xgf");

        // remove carriage returns on Windows
        generatedXml = generatedXml.replaceAll("\r", "");

        Assertions.assertEquals(expectedXml, generatedXml);
    }

    @Test
    public void testSimple() throws Exception {
        String expectedXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n" +
                "<syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\">\n" +
                "    <options>\n" +
                "        <estimationEnabled>true</estimationEnabled>\n" +
                "        <sourceListFile>sourceListFile</sourceListFile>\n" +
                "        <verify>false</verify>\n" +
                "        <threadCount>16</threadCount>\n" +
                "        <dbTable>dbTable</dbTable>\n" +
                "    </options>\n" +
                "    <source>\n" +
                "        <xGSConfig>\n" +
                "            <array>value</array>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </source>\n" +
                "    <filters>\n" +
                "        <xGFConfig>\n" +
                "            <array>value</array>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGFConfig>\n" +
                "        <xGFConfig>\n" +
                "            <array>value</array>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGFConfig>\n" +
                "    </filters>\n" +
                "    <target>\n" +
                "        <xGSConfig>\n" +
                "            <array>value</array>\n" +
                "            <number>4</number>\n" +
                "            <restrictedString>restrictedString</restrictedString>\n" +
                "            <string>string</string>\n" +
                "            <xgType>bar</xgType>\n" +
                "        </xGSConfig>\n" +
                "    </target>\n" +
                "</syncConfig>\n";

        String generatedXml = XmlGenerator.generateXml(false, false, "xgs:", "xgs:", "xgf", "xgf");

        // remove carriage returns on Windows
        generatedXml = generatedXml.replaceAll("\r", "");

        Assertions.assertEquals(expectedXml, generatedXml);
    }

    @XmlRootElement
    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    @StorageConfig(uriPrefix = "xgs:")
    @Documentation("Xml Generator Storage documentation")
    public static class XGSConfig extends AbstractConfig {
        private int number = 4;
        private String string;
        private String restrictedString;
        private Long bigNumber;
        private boolean bool = true;
        private XGType xgType = XGType.bar;
        private String[] array;

        @Option(valueHint = "yikes", description = "number description")
        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        @Option(required = true, description = "basic string")
        public String getString() {
            return string;
        }

        public void setString(String string) {
            this.string = string;
        }

        @Option(valueList = {"value1, value2, value3"}, description = "restricted string values")
        public String getRestrictedString() {
            return restrictedString;
        }

        public void setRestrictedString(String restrictedString) {
            this.restrictedString = restrictedString;
        }

        @Option(advanced = true, description = "really big number")
        public Long getBigNumber() {
            return bigNumber;
        }

        public void setBigNumber(Long bigNumber) {
            this.bigNumber = bigNumber;
        }

        @Option(advanced = true, description = "yes or no")
        public boolean isBool() {
            return bool;
        }

        public void setBool(boolean bool) {
            this.bool = bool;
        }

        @Option(description = "restricted by enum")
        public XGType getXgType() {
            return xgType;
        }

        public void setXgType(XGType xgType) {
            this.xgType = xgType;
        }

        @Option(valueHint = "value", description = "multiple values array")
        public String[] getArray() {
            return array;
        }

        public void setArray(String[] array) {
            this.array = array;
        }
    }

    @XmlRootElement
    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    @FilterConfig(cliName = "xgf")
    @Documentation("Xml Generator Filter documentation")
    public static class XGFConfig extends AbstractConfig {
        private int number = 4;
        private String string;
        private String restrictedString;
        private Long bigNumber;
        private boolean bool = true;
        private XGType xgType = XGType.bar;
        private String[] array;

        @Option(valueHint = "yikes", description = "number description")
        public int getNumber() {
            return number;
        }

        public void setNumber(int number) {
            this.number = number;
        }

        @Option(required = true, description = "basic string")
        public String getString() {
            return string;
        }

        public void setString(String string) {
            this.string = string;
        }

        @Option(valueList = {"value1, value2, value3"}, description = "restricted string values")
        public String getRestrictedString() {
            return restrictedString;
        }

        public void setRestrictedString(String restrictedString) {
            this.restrictedString = restrictedString;
        }

        @Option(advanced = true, description = "really big number")
        public Long getBigNumber() {
            return bigNumber;
        }

        public void setBigNumber(Long bigNumber) {
            this.bigNumber = bigNumber;
        }

        @Option(advanced = true, description = "yes or no")
        public boolean isBool() {
            return bool;
        }

        public void setBool(boolean bool) {
            this.bool = bool;
        }

        @Option(description = "restricted by enum")
        public XGType getXgType() {
            return xgType;
        }

        public void setXgType(XGType xgType) {
            this.xgType = xgType;
        }

        @Option(valueHint = "value", description = "multiple values array")
        public String[] getArray() {
            return array;
        }

        public void setArray(String[] array) {
            this.array = array;
        }
    }

    @XmlEnum
    public enum XGType {
        foo, bar, baz
    }
}
