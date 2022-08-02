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

import com.emc.ecs.sync.config.storage.FilesystemConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;

public class FilesystemConfigTest {
    @Test
    public void testFilesystemMarshalling() throws Exception {
        JAXBContext context = JAXBContext.newInstance(FilesystemConfig.class);

        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" +
                "<filesystemConfig xmlns=\"http://www.emc.com/ecs/sync/model\">" +
                "<deleteCheckScript>foo.sh</deleteCheckScript>" +
                "<deleteOlderThan>0</deleteOlderThan>" +
                "<excludedPaths>.*\\.bak</excludedPaths>" +
                "<excludedPaths>.*/\\.snapshot</excludedPaths>" +
                "<followLinks>true</followLinks>" +
                "<includeBaseDir>false</includeBaseDir>" +
                "<modifiedSince>2015-01-01T00:00:00Z</modifiedSince>" +
                "<path>/foo/bar</path>" +
                "<relativeLinkTargets>true</relativeLinkTargets>" +
                "<storeMetadata>true</storeMetadata>" +
                "<useAbsolutePath>true</useAbsolutePath>" +
                "</filesystemConfig>";

        FilesystemConfig object = new FilesystemConfig();
        object.setPath("/foo/bar");
        object.setUseAbsolutePath(true);
        object.setFollowLinks(true);
        object.setDeleteOlderThan(0);
        object.setDeleteCheckScript("foo.sh");
        object.setModifiedSince("2015-01-01T00:00:00Z");
        object.setExcludedPaths(new String[]{".*\\.bak", ".*/\\.snapshot"});
        object.setStoreMetadata(true);


        // unmarshall and compare to object
        Unmarshaller unmarshaller = context.createUnmarshaller();
        FilesystemConfig xObject = (FilesystemConfig) unmarshaller.unmarshal(new StringReader(xml));

        Assertions.assertEquals(object.getPath(), xObject.getPath());
        Assertions.assertEquals(object.isUseAbsolutePath(), xObject.isUseAbsolutePath());
        Assertions.assertEquals(object.isFollowLinks(), xObject.isFollowLinks());
        Assertions.assertEquals(object.getDeleteOlderThan(), xObject.getDeleteOlderThan());
        Assertions.assertEquals(object.getDeleteCheckScript(), xObject.getDeleteCheckScript());
        Assertions.assertEquals(object.getModifiedSince(), xObject.getModifiedSince());
        Assertions.assertArrayEquals(object.getExcludedPaths(), xObject.getExcludedPaths());

        // re-marshall and compare to XML
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assertions.assertEquals(xml, writer.toString());
    }
}
