package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractEndToEndTest;
import com.emc.ecs.sync.config.storage.AzureBlobConfig;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class AzureEndToEndTest extends AbstractEndToEndTest {
    @Test
    public void testAzureBlob() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();

        final String connectString = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_AZURE_BLOB_CONNECT_STRING);
        Assumptions.assumeTrue(connectString != null);
        if (!connectString.contains(";")) throw new RuntimeException("invalid export: " + connectString);

        AzureBlobConfig azureBlobConfig = new AzureBlobConfig();
        azureBlobConfig.setConnectionString(connectString);
        azureBlobConfig.setContainerName("azure-ecs-test");
        azureBlobConfig.setIncludeSnapShots(true);
        // TODO: need to implement the EndToEndTest after finish the: https://asdjira.isus.emc.com:8443/browse/ES-98
//        endToEndTest(azureBlobConfig, new TestConfig(), null, false);
    }
}
