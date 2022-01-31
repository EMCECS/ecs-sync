package com.emc.ecs.sync.ctl;

import com.emc.ecs.sync.config.XmlGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// make sure we are pulling in models from plugins
public class XmlGeneratorPluginTest {
    @Test
    public void testFilesystem() throws Exception {
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
                "        <filesystemConfig>\n" +
                "            <path>path</path>\n" +
                "            <excludedPaths>regex-pattern</excludedPaths>\n" +
                "            <includeBaseDir>false</includeBaseDir>\n" +
                "        </filesystemConfig>\n" +
                "    </source>\n" +
                "    <filters>\n" +
                "        <localCacheConfig>\n" +
                "            <localCacheRoot>cache-directory</localCacheRoot>\n" +
                "        </localCacheConfig>\n" +
                "        <idLoggingConfig>\n" +
                "            <idLogFile>path-to-file</idLogFile>\n" +
                "        </idLoggingConfig>\n" +
                "    </filters>\n" +
                "    <target>\n" +
                "        <awsS3Config>\n" +
                "            <protocol>protocol</protocol>\n" +
                "            <host>host</host>\n" +
                "            <accessKey>accessKey</accessKey>\n" +
                "            <secretKey>secretKey</secretKey>\n" +
                "            <bucketName>bucketName</bucketName>\n" +
                "            <createBucket>false</createBucket>\n" +
                "            <excludedKeys>regex-pattern</excludedKeys>\n" +
                "        </awsS3Config>\n" +
                "    </target>\n" +
                "</syncConfig>\n";

        String generatedXml = XmlGenerator.generateXml(false, false, "file:", "s3:", "local-cache", "id-logging");

        // remove carriage returns on Windows
        generatedXml = generatedXml.replaceAll("\r", "");

        Assertions.assertEquals(expectedXml, generatedXml);
    }
}
