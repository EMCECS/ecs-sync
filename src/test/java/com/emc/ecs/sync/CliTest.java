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
package com.emc.ecs.sync;

import com.emc.ecs.sync.cli.CliConfig;
import com.emc.ecs.sync.cli.CliHelper;
import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.Protocol;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.DecryptionConfig;
import com.emc.ecs.sync.config.filter.EncryptionConfig;
import com.emc.ecs.sync.config.storage.AtmosConfig;
import com.emc.ecs.sync.config.storage.AwsS3Config;
import com.emc.ecs.sync.config.storage.CasConfig;
import com.emc.ecs.sync.config.storage.FilesystemConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

public class CliTest {
    @Test
    public void testFilesystemCli() throws Exception {
        File sourceFile = new File("/tmp/foo");
        File targetFile = new File("/tmp/bar");
        String[] args = new String[]{
                "-source", "file://" + sourceFile,
                "-target", "file://" + targetFile,
                "--source-use-absolute-path"
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not FilesystemSource", source instanceof FilesystemConfig);
        FilesystemConfig fsSource = (FilesystemConfig) source;

        Object target = syncConfig.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not FilesystemTarget", target instanceof FilesystemConfig);
        FilesystemConfig fsTarget = (FilesystemConfig) target;

        Assert.assertEquals("source file mismatch", sourceFile.getPath(), fsSource.getPath());
        Assert.assertTrue("source use-absolute-path should be enabled", fsSource.isUseAbsolutePath());
        Assert.assertEquals("target file mismatch", targetFile.getPath(), fsTarget.getPath());
    }

    @Test
    public void testCasCli() throws Exception {
        String conString1 = "10.6.143.90,10.6.143.91?/file.pea";
        String conString2 = "10.6.143.97:3218,10.6.143.98:3218?name=e332b0c325c444438f51bfd8d25c6b55:test,secret=XvfPa442hanJ7GzQasyx+j5X9kY=";
        String appName = "";
        String appVersion = "";
        String deleteReason = "";

        String[] args = new String[]{
                "-source", "cas://" + conString1,
                "-target", "cas://" + conString2,
                "--source-application-name", appName,
                "--source-application-version", appVersion,
                "--source-delete-reason", deleteReason,
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not FilesystemSource", source instanceof CasConfig);
        CasConfig castSource = (CasConfig) source;

        Object target = syncConfig.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not FilesystemTarget", target instanceof CasConfig);
        CasConfig castTarget = (CasConfig) target;

        Assert.assertEquals("cas:hpp://" + conString1, ConfigUtil.generateUri(castSource));
        Assert.assertEquals("cas:hpp://" + conString2, ConfigUtil.generateUri(castTarget));
        Assert.assertEquals("source conString mismatch", "hpp://" + conString1, castSource.getConnectionString());
        Assert.assertEquals("source appName mismatch", appName, castSource.getApplicationName());
        Assert.assertEquals("source appVersion mismatch", appVersion, castSource.getApplicationVersion());
        Assert.assertEquals("source deleteReason mismatch", deleteReason, castSource.getDeleteReason());
        Assert.assertEquals("target conString mismatch", "hpp://" + conString2, castTarget.getConnectionString());
    }

//    @Test
//    public void testEcsS3Cli() throws Exception {
//        testEcsS3Parse("http", Arrays.asList("10.10.10.11", "10.10.10.12"), 80, "foo", "bar", "/baz");
//        testEcsS3Parse("http", Collections.singletonList("s3.company.com"), 9020, "wuser1@SANITY.LOCAL", "awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi", "/x/y/zee-ba-dee-ba");
//        testEcsS3Parse("http", Arrays.asList("1.6.143.97", "1.6.143.98"), 8080, "ace5d3da351242bcb095eb841ad40371/test", "HkayrXoENUQ3VCMCaaViS0tbpDs=", null);
//        testEcsS3Parse("https", Arrays.asList("10.10.10.11", "10.10.10.12", "10.10.10.13", "10.10.10.14", "10.10.10.15", "10.10.10.16", "10.10.10.17", "10.10.10.18"),
//                -1, "amz_user1234567890", "HkayrXoENUQ3VCMCaaViS0tbpDs=", "/yo/");
//
//        String sourceBucket = "source-bucket";
//        String targetBucket = "target-bucket";
//        String sourceRootKey = "source/prefix/";
//        String targetRootKey = "target/prefix/";
//
//        String[] args = new String[]{
//                "-source", "ecs-s3:http://wuser1@SANITY.LOCAL:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@10.249.237.104/" + sourceRootKey,
//                "-target", "ecs-s3:https://root:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@10.249.237.106:9021/" + targetRootKey,
//                "--source-bucket", sourceBucket,
//                "--source-decode-keys",
//                "--source-enable-vhost",
//                "--source-no-smart-client",
//                "--source-apache-client",
//                "--target-bucket", targetBucket,
//                "--target-enable-vhost",
//                "--target-no-smart-client",
//                "--target-apache-client",
//                "--s3-include-versions",
//                "--no-preserve-dirs"
//        };
//
//        // use reflection to bootstrap EcsSync using CLI arguments
//        Method optionsMethod = EcsSync.class.getDeclaredMethod("cliBootstrap", String[].class);
//        optionsMethod.setAccessible(true);
//        EcsSync sync = (EcsSync) optionsMethod.invoke(null, (Object) args);
//
//        Object source = sync.getSource();
//        Assert.assertNotNull("source is null", source);
//        Assert.assertTrue("source is not S3Source", source instanceof EcsS3Source);
//        EcsS3Source s3Source = (EcsS3Source) source;
//
//        Object target = sync.getTarget();
//        Assert.assertNotNull("target is null", target);
//        Assert.assertTrue("target is not S3Target", target instanceof EcsS3Target);
//        EcsS3Target s3Target = (EcsS3Target) target;
//
//        Assert.assertEquals("source bucket mismatch", sourceBucket, s3Source.getBucketName());
//        Assert.assertEquals(sourceRootKey, s3Source.getRootKey());
//        Assert.assertTrue("source decode-keys should be enabled", s3Source.isDecodeKeys());
//        Assert.assertTrue("source vhost should be enabled", s3Source.isEnableVHosts());
//        Assert.assertFalse("source smart-client should be disabled", s3Source.isSmartClientEnabled());
//        Assert.assertTrue("source apache-client should be enabled", s3Source.isApacheClientEnabled());
//        Assert.assertEquals("target bucket mismatch", targetBucket, s3Target.getBucketName());
//        Assert.assertEquals(targetRootKey, s3Target.getRootKey());
//        Assert.assertTrue("target vhost should be enabled", s3Target.isEnableVHosts());
//        Assert.assertFalse("target smart-client should be disabled", s3Target.isSmartClientEnabled());
//        Assert.assertTrue("target apache-client should be enabled", s3Target.isApacheClientEnabled());
//        Assert.assertTrue("target versions should be enabled", s3Target.isIncludeVersions());
//        Assert.assertFalse("target preserveDirectories should be disabled", s3Target.isPreserveDirectories());
//    }
//
//    private void testEcsS3Parse(String protocol, List<String> hosts, int port, String accessKey, String secretKey, String rootKey)
//            throws URISyntaxException {
//        String protocolStr = protocol == null ? "" : protocol + "://";
//        String portStr = port >= 0 ? ":" + port : "";
//        String hostString = SyncUtil.join(hosts, ",");
//        String uri = String.format("ecs-s3:%s%s:%s@vdc1(%s),vdc2(%s)%s%s",
//                protocolStr, accessKey, secretKey, hostString, hostString, portStr, rootKey == null ? "" : "/" + rootKey);
//
//        EcsS3Util.S3Uri s3Uri = EcsS3Util.parseUri(uri);
//
//        URI endpoint = s3Uri.getEndpointUri();
//        Assert.assertEquals("endpoint protocol different", protocol, endpoint.getScheme());
//        Assert.assertEquals("endpoint host different", hosts.get(0), endpoint.getHost());
//        Assert.assertEquals("endpoint port different", port, endpoint.getPort());
//
//        Assert.assertEquals("protocol different", protocol, s3Uri.protocol);
//        Assert.assertEquals("accessKey different", accessKey, s3Uri.accessKey);
//        Assert.assertEquals("secretKey different", secretKey, s3Uri.secretKey);
//        Assert.assertEquals("rootKey different", rootKey, s3Uri.rootKey);
//
//        Assert.assertEquals(2, s3Uri.vdcs.size());
//        Assert.assertEquals("vdc1", s3Uri.vdcs.get(0).getName());
//        Assert.assertEquals("vdc2", s3Uri.vdcs.get(1).getName());
//        Assert.assertEquals(hosts.size(), s3Uri.vdcs.get(0).getHosts().size());
//        Assert.assertEquals(hosts.size(), s3Uri.vdcs.get(1).getHosts().size());
//        for (int i = 0; i < hosts.size(); i++) {
//            Assert.assertEquals(hosts.get(i), s3Uri.vdcs.get(0).getHosts().get(i).getName());
//            Assert.assertEquals(hosts.get(i), s3Uri.vdcs.get(1).getHosts().get(i).getName());
//        }
//
//        uri = String.format("ecs-s3:%s%s:%s@%s%s%s",
//                protocolStr, accessKey, secretKey, hosts.get(0), portStr, rootKey == null ? "" : "/" + rootKey);
//
//        s3Uri = EcsS3Util.parseUri(uri);
//
//        endpoint = s3Uri.getEndpointUri();
//        Assert.assertEquals("endpoint protocol different", protocol, endpoint.getScheme());
//        Assert.assertEquals("endpoint host different", hosts.get(0), endpoint.getHost());
//        Assert.assertEquals("endpoint port different", port, endpoint.getPort());
//
//        Assert.assertEquals("protocol different", protocol, s3Uri.protocol);
//        Assert.assertEquals("accessKey different", accessKey, s3Uri.accessKey);
//        Assert.assertEquals("secretKey different", secretKey, s3Uri.secretKey);
//        Assert.assertEquals("rootKey different", rootKey, s3Uri.rootKey);
//    }

    @Test
    public void testS3Cli() throws Exception {
        String sourceAccessKey = "amz_user1234567890";
        String sourceSecret = "HkayrXoENUQ3VCMCaaViS0tbpDs=";
        String sourceBucket = "source-bucket";
        String sourceUri = String.format("s3:%s:%s@/%s", sourceAccessKey, sourceSecret, sourceBucket);

        Protocol targetProtocol = Protocol.http;
        String targetHost = "s3.company.com";
        int targetPort = 9020;
        String targetAccessKey = "wuser1@SANITY.LOCAL";
        String targetSecret = "awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi";
        String targetBucket = "target-bucket";
        String targetKeyPrefix = "target/prefix/";
        String targetUri = String.format("s3:%s://%s:%s@%s:%d/%s/%s",
                targetProtocol.toString(), targetAccessKey, targetSecret, targetHost, targetPort, targetBucket, targetKeyPrefix);
        int mpuThreshold = 100, mpuPartSize = 25, mpuThreads = 10, socketTimeout = 5000;

        String[] args = new String[]{
                "-source", sourceUri,
                "-target", targetUri,
                "--source-decode-keys",
                "--source-include-versions",
                "--target-create-bucket",
                "--target-disable-v-hosts",
                "--target-include-versions",
                "--target-legacy-signatures",
                "--target-no-preserve-directories",
                "--target-mpu-threshold-mb", "" + mpuThreshold,
                "--target-mpu-part-size-mb", "" + mpuPartSize,
                "--target-mpu-thread-count", "" + mpuThreads,
                "--target-socket-timeout-ms", "" + socketTimeout
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not AwsS3Config", source instanceof AwsS3Config);
        AwsS3Config s3Source = (AwsS3Config) source;

        Object target = syncConfig.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not AwsS3Config", target instanceof AwsS3Config);
        AwsS3Config s3Target = (AwsS3Config) target;

        Assert.assertEquals("source URI mismatch", sourceUri, s3Source.getUri());
        Assert.assertNull("source protocol should be null", s3Source.getProtocol());
        Assert.assertEquals("source access key mismatch", sourceAccessKey, s3Source.getAccessKey());
        Assert.assertEquals("source secret key mismatch", sourceSecret, s3Source.getSecretKey());
        Assert.assertNull("source host should be null", s3Source.getHost());
        Assert.assertEquals("source port mismatch", -1, s3Source.getPort());
        Assert.assertEquals("source bucket mismatch", sourceBucket, s3Source.getBucketName());
        Assert.assertNull("source keyPrefix should be null", s3Source.getKeyPrefix());
        Assert.assertTrue("source decodeKeys should be enabled", s3Source.isDecodeKeys());
        Assert.assertTrue("source includeVersions should be enabled", s3Source.isIncludeVersions());

        Assert.assertEquals("target URI mismatch", targetUri, s3Target.getUri());
        Assert.assertEquals("target protocol mismatch", targetProtocol, s3Target.getProtocol());
        Assert.assertEquals("target access key mismatch", targetAccessKey, s3Target.getAccessKey());
        Assert.assertEquals("target secret key mismatch", targetSecret, s3Target.getSecretKey());
        Assert.assertEquals("target host mismatch", targetHost, s3Target.getHost());
        Assert.assertEquals("target port mismatch", targetPort, s3Target.getPort());
        Assert.assertEquals("target bucket mismatch", targetBucket, s3Target.getBucketName());
        Assert.assertEquals("target keyPrefix mismatch", targetKeyPrefix, s3Target.getKeyPrefix());
        Assert.assertTrue("target createBucket should be enabled", s3Target.isCreateBucket());
        Assert.assertTrue("target disableVhost should be true", s3Target.isDisableVHosts());
        Assert.assertTrue("target includeVersions should be enabled", s3Target.isIncludeVersions());
        Assert.assertTrue("target legacySignatures should be enabled", s3Target.isLegacySignatures());
        Assert.assertFalse("target preserveDirectories should be disabled", s3Target.isPreserveDirectories());
        Assert.assertEquals("target MPU threshold mismatch", mpuThreshold, s3Target.getMpuThresholdMb());
        Assert.assertEquals("target MPU part size mismatch", mpuPartSize, s3Target.getMpuPartSizeMb());
        Assert.assertEquals("target MPU threads mismatch", mpuThreads, s3Target.getMpuThreadCount());
        Assert.assertEquals("target socket timeout mismatch", socketTimeout, s3Target.getSocketTimeoutMs());
    }

    @Test
    public void testAtmosCli() throws Exception {
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
                "--target-replace-metadata"
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not AtmosConfig", source instanceof AtmosConfig);
        AtmosConfig atmosSource = (AtmosConfig) source;

        Object target = syncConfig.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not AtmosConfig", target instanceof AtmosConfig);
        AtmosConfig atmosTarget = (AtmosConfig) target;

        Assert.assertEquals("source protocol mismatch", sourceProtocol, atmosSource.getProtocol());
        Assert.assertArrayEquals("source hosts mismatch", sourceHosts, atmosSource.getHosts());
        Assert.assertEquals("source port mismatch", sourcePort, atmosSource.getPort());
        Assert.assertEquals("source uid mismatch", sourceUid, atmosSource.getUid());
        Assert.assertEquals("source secret mismatch", sourceSecret, atmosSource.getSecret());
        Assert.assertEquals("source path mismatch", sourcePath, atmosSource.getPath());
        Assert.assertEquals("source accessType mismatch", sourceAccessType, atmosSource.getAccessType());
        Assert.assertTrue("source removeTagsOnDelete should be enabled", atmosSource.isRemoveTagsOnDelete());
        Assert.assertEquals("target protocol mismatch", targetProtocol, atmosTarget.getProtocol());
        Assert.assertArrayEquals("target hosts mismatch", targetHosts, atmosTarget.getHosts());
        Assert.assertEquals("target port mismatch", -1, atmosTarget.getPort());
        Assert.assertEquals("target uid mismatch", targetUid, atmosTarget.getUid());
        Assert.assertEquals("target secret mismatch", targetSecret, atmosTarget.getSecret());
        Assert.assertEquals("target path mismatch", targetPath, atmosTarget.getPath());
        Assert.assertEquals("target accessType mismatch", targetAccessType, atmosTarget.getAccessType());
        Assert.assertEquals("target wsChecksumType mismatch", targetChecksum, atmosTarget.getWsChecksumType());
        Assert.assertTrue("target replaceMetadata should be enabled", atmosTarget.isReplaceMetadata());

        // verify URI generation
        Assert.assertEquals(sourceUri, atmosSource.getUri());
        Assert.assertEquals(targetUri, atmosTarget.getUri());
    }

    @Test
    public void testEncryptDecryptCli() throws Exception {
        String encKeystore = "/tmp/store.jks";
        String encKeyPass = "foo";
        String encKeyAlias = "bar";
        String decKeystore = "/tmp/shop.jks";
        String decKeyPass = "baz";

        String args[] = new String[]{
                "-source", "file:///tmp",
                "-target", "test:",
                "-filters", "encrypt,decrypt",
                "--encrypt-keystore", encKeystore,
                "--encrypt-keystore-pass", encKeyPass,
                "--encrypt-key-alias", encKeyAlias,
                "--encrypt-force-strong",
                "--fail-if-encrypted",
                "--encrypt-update-mtime",
                "--decrypt-keystore", decKeystore,
                "--decrypt-keystore-pass", decKeyPass,
                "--fail-if-not-encrypted",
                "--decrypt-update-mtime"
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Iterator<?> filters = syncConfig.getFilters().iterator();

        Object filter = filters.next();
        Assert.assertTrue("first filter is not encryption", filter instanceof EncryptionConfig);
        EncryptionConfig encFilter = (EncryptionConfig) filter;
        Assert.assertEquals("enc keystore mismatch", encKeystore, encFilter.getEncryptKeystore());
        Assert.assertEquals("enc keystorePass mismatch", encKeyPass, encFilter.getEncryptKeystorePass());
        Assert.assertEquals("enc keyAlias mismatch", encKeyAlias, encFilter.getEncryptKeyAlias());
        Assert.assertTrue("enc forceString mismatch", encFilter.isEncryptForceStrong());
        Assert.assertTrue("enc failIfEncrypted mismatch", encFilter.isFailIfEncrypted());
        Assert.assertTrue("enc updateMtime mismatch", encFilter.isEncryptUpdateMtime());

        filter = filters.next();
        Assert.assertTrue("second filter is not decryption", filter instanceof DecryptionConfig);
        DecryptionConfig decFilter = (DecryptionConfig) filter;
        Assert.assertEquals("dec keystore mismatch", decKeystore, decFilter.getDecryptKeystore());
        Assert.assertEquals("dec keystorePass mismatch", decKeyPass, decFilter.getDecryptKeystorePass());
        Assert.assertTrue("dec failIfNotEncrypted mismatch", decFilter.isFailIfNotEncrypted());
        Assert.assertTrue("dec updateMtime mismatch", decFilter.isDecryptUpdateMtime());
    }

    @Test
    public void testCommonCli() throws Exception {
        String sourcePath = "/source/path";
        String targetPath = "/target/path";

        int bufferSize = 123456;

        String[] args = new String[]{
                "-source", "file:" + sourcePath,
                "-target", "file:" + targetPath,
                "--delete-source",
                "--no-monitor-performance",
                "--non-recursive",
                "--remember-failed",
                "--sync-acl",
                "--no-sync-data",
                "--no-sync-metadata",
                "--ignore-invalid-acls",
                "--sync-retention-expiration",
                "--force-sync",
                "--buffer-size", "" + bufferSize
        };

        CliConfig cliConfig = CliHelper.parseCliConfig(args);
        SyncConfig syncConfig = CliHelper.parseSyncConfig(cliConfig, args);

        Object source = syncConfig.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not AtmosSource", source instanceof FilesystemConfig);

        Object target = syncConfig.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not AtmosTarget", target instanceof FilesystemConfig);

        SyncOptions options = syncConfig.getOptions();
        Assert.assertTrue(options.isDeleteSource());
        Assert.assertFalse(options.isMonitorPerformance());
        Assert.assertFalse(options.isRecursive());
        Assert.assertTrue(options.isRememberFailed());
        Assert.assertTrue(options.isSyncAcl());
        Assert.assertFalse(options.isSyncData());
        Assert.assertFalse(options.isSyncMetadata());
        Assert.assertTrue("source ignoreInvalidAcls mismatch", options.isIgnoreInvalidAcls());
        Assert.assertTrue(options.isSyncRetentionExpiration());
        Assert.assertTrue("source force mismatch", options.isForceSync());
        Assert.assertEquals("source bufferSize mismatch", bufferSize, options.getBufferSize());
    }

    private String join(String[] strings, String delimiter) {
        if (strings == null) return null;
        if (strings.length == 0) return "";
        StringBuilder joined = new StringBuilder(strings[0]);
        for (int i = 1; i < strings.length; i++) {
            joined.append(delimiter).append(strings[i]);
        }
        return joined.toString();
    }
}
