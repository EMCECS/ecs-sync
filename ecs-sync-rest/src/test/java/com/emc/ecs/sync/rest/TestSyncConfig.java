/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.rest;

import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;

public class TestSyncConfig {
    @Test
    public void testMarshalling() throws Exception {
        JAXBContext context = JAXBContext.newInstance(SyncConfig.class);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                "<SyncConfig>" +
                "<Source class=\"com.emc.ecs.sync.rest.JobControl\"/>" +
                "<Target class=\"com.emc.ecs.sync.rest.JobList\">" +
                "<Property name=\"a\" value=\"b\"/>" +
                "<ListProperty name=\"list\">" +
                "<Value>har</Value>" +
                "<Value>dees</Value>" +
                "</ListProperty>" +
                "</Target>" +
                "<Filter class=\"com.emc.ecs.sync.rest.SyncConfig\">" +
                "<Property name=\"foo\" value=\"bar\"/>" +
                "</Filter>" +
                "<Filter class=\"com.emc.ecs.sync.rest.JobInfo\">" +
                "<Property name=\"baz\" value=\"bee\"/>" +
                "<Property name=\"hi\" value=\"ya\"/>" +
                "<ListProperty name=\"multi\">" +
                "<Value>value-1</Value>" +
                "<Value>value-2</Value>" +
                "<Value>value-3</Value>" +
                "</ListProperty>" +
                "</Filter>" +
                "<QueryThreadCount>2</QueryThreadCount>" +
                "<SyncThreadCount>8</SyncThreadCount>" +
                "<Recursive>false</Recursive>" +
                "<TimingsEnabled>false</TimingsEnabled>" +
                "<TimingWindow>100</TimingWindow>" +
                "<RememberFailed>true</RememberFailed>" +
                "<Verify>true</Verify>" +
                "<VerifyOnly>false</VerifyOnly>" +
                "<DeleteSource>false</DeleteSource>" +
                "<LogLevel>5</LogLevel>" +
                "<MetadataOnly>true</MetadataOnly>" +
                "<IgnoreMetadata>true</IgnoreMetadata>" +
                "<IncludeAcl>false</IncludeAcl>" +
                "<IgnoreInvalidAcls>false</IgnoreInvalidAcls>" +
                "<IncludeRetentionExpiration>true</IncludeRetentionExpiration>" +
                "<Force>true</Force>" +
                "<BufferSize>1024</BufferSize>" +
                "</SyncConfig>";

        SyncConfig object = new SyncConfig();
        object.setBufferSize(1024);
        object.setDeleteSource(false);
        object.setFilters(Arrays.asList(new PluginConfig(SyncConfig.class.getName()).addCustomProperty("foo", "bar"),
                new PluginConfig(JobInfo.class.getName()).addCustomProperty("baz", "bee").addCustomProperty("hi", "ya")
                        .addCustomListProperty("multi", "value-1", "value-2", "value-3")));
        object.setForce(true);
        object.setIgnoreInvalidAcls(false);
        object.setIgnoreMetadata(true);
        object.setIncludeAcl(false);
        object.setIncludeRetentionExpiration(true);
        object.setLogLevel("5");
        object.setMetadataOnly(true);
        object.setQueryThreadCount(2);
        object.setRecursive(false);
        object.setRememberFailed(true);
        object.setSource(new PluginConfig(JobControl.class.getName()));
        object.setTarget(new PluginConfig(JobList.class.getName()).addCustomProperty("a", "b").addCustomListProperty("list", "har", "dees"));
        object.setSyncThreadCount(8);
        object.setTimingsEnabled(false);
        object.setTimingWindow(100);
        object.setVerify(true);
        object.setVerifyOnly(false);

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncConfig unmarshalledObject = (SyncConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assert.assertEquals(object.getBufferSize(), unmarshalledObject.getBufferSize());
        Assert.assertEquals(object.getDeleteSource(), unmarshalledObject.getDeleteSource());
        Assert.assertEquals(object.getFilters().size(), unmarshalledObject.getFilters().size());
        for (int i = 0; i < object.getFilters().size(); i++) {
            Assert.assertEquals(object.getFilters().get(i).getPluginClass(), unmarshalledObject.getFilters().get(i).getPluginClass());
            Assert.assertEquals(object.getFilters().get(i).getCustomProperties(), unmarshalledObject.getFilters().get(i).getCustomProperties());
            Assert.assertEquals(object.getFilters().get(i).getCustomListProperties(), unmarshalledObject.getFilters().get(i).getCustomListProperties());
        }
        Assert.assertEquals(object.getForce(), unmarshalledObject.getForce());
        Assert.assertEquals(object.getIgnoreInvalidAcls(), unmarshalledObject.getIgnoreInvalidAcls());
        Assert.assertEquals(object.getIgnoreMetadata(), unmarshalledObject.getIgnoreMetadata());
        Assert.assertEquals(object.getIncludeAcl(), unmarshalledObject.getIncludeAcl());
        Assert.assertEquals(object.getIncludeRetentionExpiration(), unmarshalledObject.getIncludeRetentionExpiration());
        Assert.assertEquals(object.getLogLevel(), unmarshalledObject.getLogLevel());
        Assert.assertEquals(object.getMetadataOnly(), unmarshalledObject.getMetadataOnly());
        Assert.assertEquals(object.getQueryThreadCount(), unmarshalledObject.getQueryThreadCount());
        Assert.assertEquals(object.getRecursive(), unmarshalledObject.getRecursive());
        Assert.assertEquals(object.getRememberFailed(), unmarshalledObject.getRememberFailed());
        Assert.assertEquals(object.getSource().getPluginClass(), unmarshalledObject.getSource().getPluginClass());
        Assert.assertEquals(object.getSource().getCustomProperties(), unmarshalledObject.getSource().getCustomProperties());
        Assert.assertEquals(object.getSource().getCustomListProperties(), unmarshalledObject.getSource().getCustomListProperties());
        Assert.assertEquals(object.getSyncThreadCount(), unmarshalledObject.getSyncThreadCount());
        Assert.assertEquals(object.getTarget().getPluginClass(), unmarshalledObject.getTarget().getPluginClass());
        Assert.assertEquals(object.getTarget().getCustomProperties(), unmarshalledObject.getTarget().getCustomProperties());
        Assert.assertEquals(object.getTarget().getCustomListProperties(), unmarshalledObject.getTarget().getCustomListProperties());
        Assert.assertEquals(object.getTimingsEnabled(), unmarshalledObject.getTimingsEnabled());
        Assert.assertEquals(object.getTimingWindow(), unmarshalledObject.getTimingWindow());
        Assert.assertEquals(object.getVerify(), unmarshalledObject.getVerify());
        Assert.assertEquals(object.getVerifyOnly(), unmarshalledObject.getVerifyOnly());

        // re-marshall and compare to XML
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(unmarshalledObject, writer);
        Assert.assertEquals(xml, writer.toString());
    }

    @Test
    public void testEmpty() throws Exception {
        JAXBContext context = JAXBContext.newInstance(SyncConfig.class);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><SyncConfig/>";

        SyncConfig object = new SyncConfig();

        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        SyncConfig unmarshalledObject = (SyncConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assert.assertEquals(object.getBufferSize(), unmarshalledObject.getBufferSize());
        Assert.assertEquals(object.getDeleteSource(), unmarshalledObject.getDeleteSource());
        Assert.assertEquals(object.getFilters().size(), unmarshalledObject.getFilters().size());
        for (int i = 0; i < object.getFilters().size(); i++) {
            Assert.assertEquals(object.getFilters().get(i).getPluginClass(), unmarshalledObject.getFilters().get(i).getPluginClass());
            Assert.assertEquals(object.getFilters().get(i).getCustomProperties(), unmarshalledObject.getFilters().get(i).getCustomProperties());
        }
        Assert.assertEquals(object.getForce(), unmarshalledObject.getForce());
        Assert.assertEquals(object.getIgnoreInvalidAcls(), unmarshalledObject.getIgnoreInvalidAcls());
        Assert.assertEquals(object.getIgnoreMetadata(), unmarshalledObject.getIgnoreMetadata());
        Assert.assertEquals(object.getIncludeAcl(), unmarshalledObject.getIncludeAcl());
        Assert.assertEquals(object.getIncludeRetentionExpiration(), unmarshalledObject.getIncludeRetentionExpiration());
        Assert.assertEquals(object.getLogLevel(), unmarshalledObject.getLogLevel());
        Assert.assertEquals(object.getMetadataOnly(), unmarshalledObject.getMetadataOnly());
        Assert.assertEquals(object.getQueryThreadCount(), unmarshalledObject.getQueryThreadCount());
        Assert.assertEquals(object.getRecursive(), unmarshalledObject.getRecursive());
        Assert.assertEquals(object.getRememberFailed(), unmarshalledObject.getRememberFailed());
        Assert.assertEquals(object.getSource(), unmarshalledObject.getSource());
        Assert.assertEquals(object.getSyncThreadCount(), unmarshalledObject.getSyncThreadCount());
        Assert.assertEquals(object.getTarget(), unmarshalledObject.getTarget());
        Assert.assertEquals(object.getTimingsEnabled(), unmarshalledObject.getTimingsEnabled());
        Assert.assertEquals(object.getTimingWindow(), unmarshalledObject.getTimingWindow());
        Assert.assertEquals(object.getVerify(), unmarshalledObject.getVerify());
        Assert.assertEquals(object.getVerifyOnly(), unmarshalledObject.getVerifyOnly());

        // re-marshall and compare to XML
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assert.assertEquals(xml, writer.toString());
    }
}
