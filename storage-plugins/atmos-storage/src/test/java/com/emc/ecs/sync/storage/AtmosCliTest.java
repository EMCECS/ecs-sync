package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractCliTest;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.AtmosConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AtmosCliTest extends AbstractCliTest {
    @Test
    public void testAtmosCli() {
        Protocol sourceProtocol = Protocol.http;
        String[] sourceHosts = new String[]{"10.6.143.97", "10.6.143.98", "10.6.143.99", "10.6.143.100"};
        int sourcePort = 8080;
        String sourceUid = "ace5d3da351242bcb095eb841ad40371/test";
        String sourceSecret = "HkayrXoENUQ3VCMCaaViS0tbpDs=";
        String sourcePath = "/baz";
        String sourceUri = "atmos:" + sourceProtocol + "://" + sourceUid + ":" + sourceSecret + "@10.6.143.97,10.6.143.98,10.6.143.99,10.6.143.100:" + sourcePort + sourcePath;
        AtmosConfig.AccessType sourceAccessType = AtmosConfig.AccessType.namespace;

        Protocol targetProtocol = Protocol.https;
        String[] targetHosts = new String[]{"atmos.company.com"};
        String targetUid = "wuser1@SANITY.LOCAL";
        String targetSecret = "awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi";
        String targetPath = "/my/data/dir/";
        String targetUri = "atmos:" + targetProtocol + "://" + targetUid + ":" + targetSecret + "@atmos.company.com" + targetPath;
        AtmosConfig.Hash targetChecksum = AtmosConfig.Hash.md5;
        AtmosConfig.AccessType targetAccessType = AtmosConfig.AccessType.namespace;

        String[] args = new String[]{
                "-source", sourceUri,
                "-target", targetUri,
                "--source-access-type", sourceAccessType.toString(),
                "--source-remove-tags-on-delete",
                "--target-access-type", targetAccessType.toString(),
                "--target-ws-checksum-type", targetChecksum.toString(),
                "--target-replace-metadata",
                "--target-preserve-object-id"
        };

        SyncConfig syncConfig = parseSyncConfig(args);

        Object source = syncConfig.getSource();
        Assertions.assertNotNull(source, "source is null");
        Assertions.assertTrue(source instanceof AtmosConfig, "source is not AtmosConfig");
        AtmosConfig atmosSource = (AtmosConfig) source;

        Object target = syncConfig.getTarget();
        Assertions.assertNotNull(target, "target is null");
        Assertions.assertTrue(target instanceof AtmosConfig, "target is not AtmosConfig");
        AtmosConfig atmosTarget = (AtmosConfig) target;

        Assertions.assertEquals(sourceProtocol, atmosSource.getProtocol(), "source protocol mismatch");
        Assertions.assertArrayEquals(sourceHosts, atmosSource.getHosts(), "source hosts mismatch");
        Assertions.assertEquals(sourcePort, atmosSource.getPort(), "source port mismatch");
        Assertions.assertEquals(sourceUid, atmosSource.getUid(), "source uid mismatch");
        Assertions.assertEquals(sourceSecret, atmosSource.getSecret(), "source secret mismatch");
        Assertions.assertEquals(sourcePath, atmosSource.getPath(), "source path mismatch");
        Assertions.assertEquals(sourceAccessType, atmosSource.getAccessType(), "source accessType mismatch");
        Assertions.assertTrue(atmosSource.isRemoveTagsOnDelete(), "source removeTagsOnDelete should be enabled");
        Assertions.assertEquals(targetProtocol, atmosTarget.getProtocol(), "target protocol mismatch");
        Assertions.assertArrayEquals(targetHosts, atmosTarget.getHosts(), "target hosts mismatch");
        Assertions.assertEquals(-1, atmosTarget.getPort(), "target port mismatch");
        Assertions.assertEquals(targetUid, atmosTarget.getUid(), "target uid mismatch");
        Assertions.assertEquals(targetSecret, atmosTarget.getSecret(), "target secret mismatch");
        Assertions.assertEquals(targetPath, atmosTarget.getPath(), "target path mismatch");
        Assertions.assertEquals(targetAccessType, atmosTarget.getAccessType(), "target accessType mismatch");
        Assertions.assertEquals(targetChecksum, atmosTarget.getWsChecksumType(), "target wsChecksumType mismatch");
        Assertions.assertTrue(atmosTarget.isReplaceMetadata(), "target replaceMetadata should be enabled");
        Assertions.assertTrue(atmosTarget.isPreserveObjectId(), "target preserveObjectId should be enabled");

        // verify URI generation
        Assertions.assertEquals(sourceUri, atmosSource.getUri(false));
        Assertions.assertEquals(targetUri, atmosTarget.getUri(false));
    }
}
