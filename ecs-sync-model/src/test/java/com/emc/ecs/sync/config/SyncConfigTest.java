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
package com.emc.ecs.sync.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;

public class SyncConfigTest {

    @Test
    public void testMarshalling() throws Exception {
        JAXBContext context = JAXBContext.newInstance(SyncConfig.class, TestStorageConfig.class, TestFilterConfig.class);
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\">" +
                "<options>" +
                "<bufferSize>524288</bufferSize>" +
                "<dbEnhancedDetailsEnabled>false</dbEnhancedDetailsEnabled>" +
                "<deleteSource>false</deleteSource>" +
                "<estimationEnabled>true</estimationEnabled>" +
                "<forceSync>false</forceSync>" +
                "<ignoreInvalidAcls>false</ignoreInvalidAcls>" +
                "<monitorPerformance>true</monitorPerformance>" +
                "<recursive>true</recursive>" +
                "<rememberFailed>false</rememberFailed>" +
                "<retryAttempts>2</retryAttempts>" +
                "<sourceList><![CDATA[line1\n" +
                "line2\n" +
                "line3]]></sourceList>" +
                "<sourceListFile>/my/source/list/file</sourceListFile>" +
                "<sourceListRawValues>true</sourceListRawValues>" +
                "<syncAcl>false</syncAcl>" +
                "<syncData>true</syncData>" +
                "<syncMetadata>true</syncMetadata>" +
                "<syncRetentionExpiration>false</syncRetentionExpiration>" +
                "<threadCount>16</threadCount>" +
                "<timingWindow>1000</timingWindow>" +
                "<timingsEnabled>false</timingsEnabled>" +
                "<useMetadataChecksumForVerification>false</useMetadataChecksumForVerification>" +
                "<verify>false</verify>" +
                "<verifyOnly>false</verifyOnly>" +
                "</options>" +
                "<source><testStorageConfig><location>foo</location></testStorageConfig></source>" +
                "<filters>" +
                "<testFilterConfig><action>baz</action></testFilterConfig>" +
                "</filters>" +
                "<target><testStorageConfig><location>bar</location></testStorageConfig></target>" +
                "</syncConfig>";

        SyncConfig object = new SyncConfig();

        TestStorageConfig source = new TestStorageConfig();
        source.setLocation("foo");

        TestStorageConfig target = new TestStorageConfig();
        target.setLocation("bar");

        TestFilterConfig filter = new TestFilterConfig();
        filter.setAction("baz");

        SyncOptions options = new SyncOptions();
        options.setBufferSize(524288);
        options.setSourceList(new String[]{"line1", "line2", "line3"});
        options.setSourceListFile("/my/source/list/file");
        options.setSourceListRawValues(true);

        object.setSource(source);
        object.setTarget(target);
        object.setFilters(Collections.singletonList((Object) filter));
        object.setOptions(options);

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncConfig xObject = (SyncConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assertions.assertEquals(((TestStorageConfig) object.getSource()).getLocation(), ((TestStorageConfig) xObject.getSource()).getLocation());
        Assertions.assertEquals(((TestStorageConfig) object.getTarget()).getLocation(), ((TestStorageConfig) xObject.getTarget()).getLocation());
        Assertions.assertEquals(((TestFilterConfig) object.getFilters().get(0)).getAction(), ((TestFilterConfig) xObject.getFilters().get(0)).getAction());

        SyncOptions xOptions = xObject.getOptions();
        Assertions.assertEquals(options.getBufferSize(), xOptions.getBufferSize());
        Assertions.assertEquals(options.isDeleteSource(), xOptions.isDeleteSource());
        Assertions.assertEquals(options.isEstimationEnabled(), xOptions.isEstimationEnabled());
        Assertions.assertEquals(options.isForceSync(), xOptions.isForceSync());
        Assertions.assertEquals(options.isIgnoreInvalidAcls(), xOptions.isIgnoreInvalidAcls());
        Assertions.assertEquals(options.isMonitorPerformance(), xOptions.isMonitorPerformance());
        Assertions.assertEquals(options.isRecursive(), xOptions.isRecursive());
        Assertions.assertEquals(options.isRememberFailed(), xOptions.isRememberFailed());
        Assertions.assertEquals(options.getRetryAttempts(), xOptions.getRetryAttempts());
        Assertions.assertArrayEquals(options.getSourceList(), xOptions.getSourceList());
        Assertions.assertEquals(options.getSourceListFile(), xOptions.getSourceListFile());
        Assertions.assertEquals(options.isSourceListRawValues(), xOptions.isSourceListRawValues());
        Assertions.assertEquals(options.isSyncAcl(), xOptions.isSyncAcl());
        Assertions.assertEquals(options.isSyncData(), xOptions.isSyncData());
        Assertions.assertEquals(options.isSyncRetentionExpiration(), xOptions.isSyncRetentionExpiration());
        Assertions.assertEquals(options.isSyncMetadata(), xOptions.isSyncMetadata());
        Assertions.assertEquals(options.getThreadCount(), xOptions.getThreadCount());
        Assertions.assertEquals(options.getTimingWindow(), xOptions.getTimingWindow());
        Assertions.assertEquals(options.isTimingsEnabled(), xOptions.isTimingsEnabled());
        Assertions.assertEquals(options.isVerify(), xOptions.isVerify());
        Assertions.assertEquals(options.isVerifyOnly(), xOptions.isVerifyOnly());

        // re-marshall and compare to XML
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assertions.assertEquals(xml, writer.toString());
    }

    @Test
    public void testCDataEscape() throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<syncOptions xmlns=\"http://www.emc.com/ecs/sync/model\">" +
                "<bufferSize>524288</bufferSize>" +
                "<dbEnhancedDetailsEnabled>false</dbEnhancedDetailsEnabled>" +
                "<deleteSource>false</deleteSource>" +
                "<estimationEnabled>true</estimationEnabled>" +
                "<forceSync>false</forceSync>" +
                "<ignoreInvalidAcls>false</ignoreInvalidAcls>" +
                "<monitorPerformance>true</monitorPerformance>" +
                "<recursive>true</recursive>" +
                "<rememberFailed>false</rememberFailed>" +
                "<retryAttempts>2</retryAttempts>" +
                "<sourceList><![CDATA[one[bracket\n" +
                "two[[brackets\n" +
                "close]bracket\n" +
                "two]]closes\n" +
                "<![CDATA[\n" +
                "]]]]><![CDATA[>]]></sourceList>" +
                "<sourceListRawValues>false</sourceListRawValues>" +
                "<syncAcl>false</syncAcl>" +
                "<syncData>true</syncData>" +
                "<syncMetadata>true</syncMetadata>" +
                "<syncRetentionExpiration>false</syncRetentionExpiration>" +
                "<threadCount>16</threadCount>" +
                "<timingWindow>1000</timingWindow>" +
                "<timingsEnabled>false</timingsEnabled>" +
                "<useMetadataChecksumForVerification>false</useMetadataChecksumForVerification>" +
                "<verify>false</verify>" +
                "<verifyOnly>false</verifyOnly>" +
                "</syncOptions>";


        SyncOptions object = new SyncOptions();
        object.setBufferSize(524288);
        object.setSourceList(new String[]{"one[bracket", "two[[brackets", "close]bracket", "two]]closes", "<![CDATA[", "]]>"});

        JAXBContext context = JAXBContext.newInstance(SyncOptions.class);

        // marshall and compare to XML
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assertions.assertEquals(xml, writer.toString());

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncOptions xObject = (SyncOptions) unmarshaller.unmarshal(new StringReader(xml));

        Assertions.assertArrayEquals(object.getSourceList(), xObject.getSourceList());
    }

    @Test
    public void testEmpty() throws Exception {
        JAXBContext context = JAXBContext.newInstance(SyncConfig.class);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\"/>";

        SyncConfig object = new SyncConfig();

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncConfig xObject = (SyncConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assertions.assertEquals(object.getSource(), xObject.getSource());
        Assertions.assertEquals(object.getTarget(), xObject.getTarget());
        Assertions.assertEquals(object.getFilters(), xObject.getFilters());
        Assertions.assertEquals(object.getOptions(), xObject.getOptions());

        // re-marshall and compare to XML
        // need to null out options since they were generated as defaults
        object.setOptions(null);
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assertions.assertEquals(xml, writer.toString());
    }

    @Test
    public void testProperties() throws Exception {
        JAXBContext context = JAXBContext.newInstance(SyncConfig.class);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\">" +
                "<properties>" +
                "<entry><key>bim</key><value>bam</value></entry>" +
                "<entry><key>foo</key><value>bar</value></entry>" +
                "<entry><key>shave-and-a-hair-cut</key><value>two-bits</value></entry>" +
                "</properties>" +
                "</syncConfig>";

        SyncConfig object = new SyncConfig();
        object.withProperty("foo", "bar").withProperty("bim", "bam").withProperty("shave-and-a-hair-cut", "two-bits");

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncConfig xObject = (SyncConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assertions.assertEquals(object.getSource(), xObject.getSource());
        Assertions.assertEquals(object.getTarget(), xObject.getTarget());
        Assertions.assertEquals(object.getFilters(), xObject.getFilters());
        Assertions.assertEquals(object.getOptions(), xObject.getOptions());
        Assertions.assertEquals(object.getProperties(), xObject.getProperties());

        // re-marshall and compare to XML
        // need to null out options since they were generated as defaults
        object.setOptions(null);
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assertions.assertEquals(xml, writer.toString());
    }

    @XmlRootElement
    static class TestStorageConfig {
        private String location;

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }

    @XmlRootElement
    static class TestFilterConfig {
        private String action;

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }
    }
}
