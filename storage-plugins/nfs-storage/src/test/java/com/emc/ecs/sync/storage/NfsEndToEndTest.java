package com.emc.ecs.sync.storage;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.emc.ecs.nfsclient.rpc.CredentialUnix;
import com.emc.ecs.sync.AbstractEndToEndTest;
import com.emc.ecs.sync.config.storage.NfsConfig;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectMetadata;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class NfsEndToEndTest extends AbstractEndToEndTest {
    @Test
    public void testNfs() throws Exception {
        Properties syncProperties = com.emc.ecs.sync.test.TestConfig.getProperties();
        String export = syncProperties.getProperty(com.emc.ecs.sync.test.TestConfig.PROP_NFS_EXPORT);
        Assumptions.assumeTrue(export != null);
        if (!export.contains(":")) throw new RuntimeException("invalid export: " + export);
        String server = export.split(":")[0];
        String mountPath = export.split(":")[1];

        final Nfs3 nfs = new Nfs3(server, mountPath, new CredentialUnix(0, 0, null), 3);
        final Nfs3File tempDir = new Nfs3File(nfs, "/ecs-sync-nfs-test");
        tempDir.mkdir();
        if (!tempDir.exists() || !tempDir.isDirectory()) {
            throw new RuntimeException("unable to make temp dir");
        }

        try {
            NfsConfig config = new NfsConfig();
            config.setServer(server);
            config.setMountPath(mountPath);
            config.setSubPath(tempDir.getPath().substring(1));
            config.setStoreMetadata(true);

            multiEndToEndTest(config, new TestConfig(), false);
        } finally {
            try {
                Nfs3File metaFile = tempDir.getChildFile(ObjectMetadata.METADATA_DIR);
                if (metaFile.exists()) {
                    metaFile.delete();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            tempDir.delete();
            assertFalse(tempDir.exists());
        }
    }
}
