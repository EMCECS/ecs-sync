package com.emc.ecs.sync.config;

import com.emc.ecs.sync.config.storage.FilesystemConfig;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class StorageConfigTest {
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
                "<modifiedSince>2015-01-01T00:00:00Z</modifiedSince>" +
                "<path>/foo/bar</path>" +
                "<storeMetadata>true</storeMetadata>" +
                "<useAbsolutePath>true</useAbsolutePath>" +
                "</filesystemConfig>";

        Map<String, String> mimeMap = new HashMap<>();
        mimeMap.put("foo", "x-bar");
        mimeMap.put("aaa", "application/octet-stream");
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

        Assert.assertEquals(object.getPath(), xObject.getPath());
        Assert.assertEquals(object.isUseAbsolutePath(), xObject.isUseAbsolutePath());
        Assert.assertEquals(object.isFollowLinks(), xObject.isFollowLinks());
        Assert.assertEquals(object.getDeleteOlderThan(), xObject.getDeleteOlderThan());
        Assert.assertEquals(object.getDeleteCheckScript(), xObject.getDeleteCheckScript());
        Assert.assertEquals(object.getModifiedSince(), xObject.getModifiedSince());
        Assert.assertArrayEquals(object.getExcludedPaths(), xObject.getExcludedPaths());

        // re-marshall and compare to XML
        Marshaller marshaller = context.createMarshaller();
        StringWriter writer = new StringWriter();
        marshaller.marshal(object, writer);
        Assert.assertEquals(xml, writer.toString());
    }
}
