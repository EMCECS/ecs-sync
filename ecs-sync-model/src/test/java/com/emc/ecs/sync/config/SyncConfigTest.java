/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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

import org.junit.Assert;
import org.junit.Test;

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
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                "<syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\">" +
                "<options>" +
                "<bufferSize>524288</bufferSize>" +
                "<deleteSource>false</deleteSource>" +
                "<forceSync>false</forceSync>" +
                "<ignoreInvalidAcls>false</ignoreInvalidAcls>" +
                "<monitorPerformance>true</monitorPerformance>" +
                "<recursive>true</recursive>" +
                "<rememberFailed>false</rememberFailed>" +
                "<retryAttempts>2</retryAttempts>" +
                "<syncAcl>false</syncAcl>" +
                "<syncData>true</syncData>" +
                "<syncMetadata>true</syncMetadata>" +
                "<syncRetentionExpiration>false</syncRetentionExpiration>" +
                "<threadCount>16</threadCount>" +
                "<timingWindow>1000</timingWindow>" +
                "<timingsEnabled>false</timingsEnabled>" +
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

        object.setSource(source);
        object.setTarget(target);
        object.setFilters(Collections.singletonList((Object) filter));
        object.setOptions(options);

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncConfig xObject = (SyncConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assert.assertEquals(((TestStorageConfig) object.getSource()).getLocation(), ((TestStorageConfig) xObject.getSource()).getLocation());
        Assert.assertEquals(((TestStorageConfig) object.getTarget()).getLocation(), ((TestStorageConfig) xObject.getTarget()).getLocation());
        Assert.assertEquals(((TestFilterConfig) object.getFilters().get(0)).getAction(), ((TestFilterConfig) xObject.getFilters().get(0)).getAction());

        SyncOptions xOptions = xObject.getOptions();
        Assert.assertEquals(options.getBufferSize(), xOptions.getBufferSize());
        Assert.assertEquals(options.isDeleteSource(), xOptions.isDeleteSource());
        Assert.assertEquals(options.isForceSync(), xOptions.isForceSync());
        Assert.assertEquals(options.isIgnoreInvalidAcls(), xOptions.isIgnoreInvalidAcls());
        Assert.assertEquals(options.isMonitorPerformance(), xOptions.isMonitorPerformance());
        Assert.assertEquals(options.isRecursive(), xOptions.isRecursive());
        Assert.assertEquals(options.isRememberFailed(), xOptions.isRememberFailed());
        Assert.assertEquals(options.getRetryAttempts(), xOptions.getRetryAttempts());
        Assert.assertEquals(options.isSyncAcl(), xOptions.isSyncAcl());
        Assert.assertEquals(options.isSyncData(), xOptions.isSyncData());
        Assert.assertEquals(options.isSyncRetentionExpiration(), xOptions.isSyncRetentionExpiration());
        Assert.assertEquals(options.isSyncMetadata(), xOptions.isSyncMetadata());
        Assert.assertEquals(options.getThreadCount(), xOptions.getThreadCount());
        Assert.assertEquals(options.getTimingWindow(), xOptions.getTimingWindow());
        Assert.assertEquals(options.isTimingsEnabled(), xOptions.isTimingsEnabled());
        Assert.assertEquals(options.isVerify(), xOptions.isVerify());
        Assert.assertEquals(options.isVerifyOnly(), xOptions.isVerifyOnly());

        // re-marshall and compare to XML
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assert.assertEquals(xml, writer.toString());
    }

    @Test
    public void testEmpty() throws Exception {
        JAXBContext context = JAXBContext.newInstance(SyncConfig.class);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><syncConfig xmlns=\"http://www.emc.com/ecs/sync/model\"/>";

        SyncConfig object = new SyncConfig();

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncConfig xObject = (SyncConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assert.assertEquals(object.getSource(), xObject.getSource());
        Assert.assertEquals(object.getTarget(), xObject.getTarget());
        Assert.assertEquals(object.getFilters(), xObject.getFilters());
        Assert.assertEquals(object.getOptions(), xObject.getOptions());

        // re-marshall and compare to XML
        // need to null out options since they were generated as defaults
        object.setOptions(null);
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assert.assertEquals(xml, writer.toString());
    }

    @Test
    public void testProperties() throws Exception {
        JAXBContext context = JAXBContext.newInstance(SyncConfig.class);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
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

        Assert.assertEquals(object.getSource(), xObject.getSource());
        Assert.assertEquals(object.getTarget(), xObject.getTarget());
        Assert.assertEquals(object.getFilters(), xObject.getFilters());
        Assert.assertEquals(object.getOptions(), xObject.getOptions());
        Assert.assertEquals(object.getProperties(), xObject.getProperties());

        // re-marshall and compare to XML
        // need to null out options since they were generated as defaults
        object.setOptions(null);
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assert.assertEquals(xml, writer.toString());
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
