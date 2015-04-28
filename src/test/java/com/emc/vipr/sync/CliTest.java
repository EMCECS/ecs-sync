/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync;

import com.emc.vipr.sync.filter.DecryptionFilter;
import com.emc.vipr.sync.filter.EncryptionFilter;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.source.AtmosSource;
import com.emc.vipr.sync.source.FilesystemSource;
import com.emc.vipr.sync.source.S3Source;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.AtmosTarget;
import com.emc.vipr.sync.target.FilesystemTarget;
import com.emc.vipr.sync.target.S3Target;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.AtmosUtil;
import com.emc.vipr.sync.util.S3Util;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

public class CliTest {
    @Test
    public void testFilesystemCli() throws Exception {
        File sourceFile = new File("/tmp/foo");
        File targetFile = new File("/tmp/bar");
        String[] args = new String[]{
                "-source", "file://" + sourceFile,
                "-target", "file://" + targetFile,
                "--use-absolute-path"
        };

        // use reflection to bootstrap ViPRSync using CLI arguments
        Method optionsMethod = ViPRSync.class.getDeclaredMethod("cliBootstrap", String[].class);
        optionsMethod.setAccessible(true);
        ViPRSync sync = (ViPRSync) optionsMethod.invoke(null, (Object) args);

        Object source = sync.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not FilesystemSource", source instanceof FilesystemSource);
        FilesystemSource fsSource = (FilesystemSource) source;

        Object target = sync.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not FilesystemTarget", target instanceof FilesystemTarget);
        FilesystemTarget fsTarget = (FilesystemTarget) target;

        Assert.assertEquals("source file mismatch", sourceFile, fsSource.getRootFile());
        Assert.assertTrue("source use-absolute-path should be enabled", fsSource.isUseAbsolutePath());
        Assert.assertEquals("target file mismatch", targetFile, fsTarget.getTargetRoot());
    }

    @Test
    public void testS3Cli() throws Exception {
        testS3Parse("http", "s3.company.com", 80, "foo", "bar", "/baz");
        testS3Parse("http", "10.249.237.104", 9020, "wuser1@SANITY.LOCAL", "awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi", "/x/y/zee-ba-dee-ba");
        testS3Parse("http", "10.6.143.97", 8080, "ace5d3da351242bcb095eb841ad40371/test", "HkayrXoENUQ3VCMCaaViS0tbpDs=", null);
        testS3Parse(null, null, -1, "amz_user1234567890", "HkayrXoENUQ3VCMCaaViS0tbpDs=", "/yo/");

        String sourceBucket = "source-bucket";
        String targetBucket = "target-bucket";

        String[] args = new String[]{
                "-source", "s3:http://wuser1@SANITY.LOCAL:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@10.249.237.104:9020/source/prefix/",
                "-target", "s3:https://root:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@10.249.237.106:9021/target/prefix/",
                "--source-bucket", sourceBucket,
                "--source-decode-keys",
                "--source-disable-vhost",
                "--target-bucket", targetBucket,
                "--target-disable-vhost"
        };

        // use reflection to bootstrap ViPRSync using CLI arguments
        Method optionsMethod = ViPRSync.class.getDeclaredMethod("cliBootstrap", String[].class);
        optionsMethod.setAccessible(true);
        ViPRSync sync = (ViPRSync) optionsMethod.invoke(null, (Object) args);

        Object source = sync.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not S3Source", source instanceof S3Source);
        S3Source s3Source = (S3Source) source;

        Object target = sync.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not S3Target", target instanceof S3Target);
        S3Target s3Target = (S3Target) target;

        Assert.assertEquals("source bucket mismatch", sourceBucket, s3Source.getBucketName());
        Assert.assertTrue("source decode-keys should be enabled", s3Source.isDecodeKeys());
        Assert.assertEquals("target bucket mismatch", targetBucket, s3Target.getBucketName());
    }

    private void testS3Parse(String protocol, String host, int port, String accessKey, String secretKey, String rootKey)
            throws URISyntaxException {
        String protocolStr = protocol == null ? "" : protocol + "://";
        String portStr = port >= 0 ? ":" + port : "";
        String uri = String.format("s3:%s%s:%s@%s%s%s",
                protocolStr, accessKey, secretKey, host == null ? "" : host, portStr, rootKey == null ? "" : rootKey);

        S3Util.S3Uri s3Uri = S3Util.parseUri(uri);

        if (host != null) {
            URI endpoint = new URI(s3Uri.endpoint);
            Assert.assertEquals("protocol different", protocol, endpoint.getScheme());
            Assert.assertEquals("host different", host, endpoint.getHost());
            Assert.assertEquals("port different", port, endpoint.getPort());
        }
        Assert.assertEquals("protocol different", protocol, s3Uri.protocol);
        Assert.assertEquals("accessKey different", accessKey, s3Uri.accessKey);
        Assert.assertEquals("secretKey different", secretKey, s3Uri.secretKey);
        Assert.assertEquals("rootKey different", rootKey, s3Uri.rootKey);
    }

    @Test
    public void testAtmosCli() throws Exception {
        testAtmosParse("http", new String[]{"10.6.143.97", "10.6.143.98", "10.6.143.99", "10.6.143.100"}, 8080,
                "ace5d3da351242bcb095eb841ad40371/test", "HkayrXoENUQ3VCMCaaViS0tbpDs=", "/baz");
        testAtmosParse("http", new String[]{"atmos.company.com"}, -1,
                "wuser1@SANITY.LOCAL", "awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi", "/my/data/dir/");

        String sourcePath = "/source/path";
        String targetPath = "/target/path";
        String sourceOidList = "/path/to/oidfile.lst";
        String sourceNameList = "/path/to/namefile.lst";
        String sourceSqlQuery = "select x, y, z from the_table where a = ? and b = 'foo'";
        String jdbcUrl = "jdbc:vipr:@viprserver:1234";
        String jdbcClass = "com.emc.vipr.ViPRDriver";
        String jdbcUser = "johndoe";
        String jdbcPassword = "myPassword#1!";
        String targetChecksum = "MD5";
        String retDelayWindow = "1";

        String[] args = new String[]{
                "-source", "atmos:http://ace5d3da351242bcb095eb841ad40371/test:HkayrXoENUQ3VCMCaaViS0tbpDs=@10.6.143.97,10.6.143.98,10.6.143.99,10.6.143.100" + sourcePath,
                "-target", "atmos:https://wuser1@SANITY.LOCAL:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@10.249.237.105,10.249.237.106:9023" + targetPath,
                "--source-oid-list", sourceOidList,
                "--source-name-list", sourceNameList,
                "--source-sql-query", sourceSqlQuery,
                "--query-jdbc-url", jdbcUrl,
                "--query-jdbc-driver-class", jdbcClass,
                "--query-user", jdbcUser,
                "--query-password", jdbcPassword,
                "--remove-tags-on-delete",
                "--no-update",
                "--target-checksum", targetChecksum,
                "--retention-delay-window", retDelayWindow
        };

        // use reflection to bootstrap ViPRSync using CLI arguments
        Method optionsMethod = ViPRSync.class.getDeclaredMethod("cliBootstrap", String[].class);
        optionsMethod.setAccessible(true);
        ViPRSync sync = (ViPRSync) optionsMethod.invoke(null, (Object) args);

        Object source = sync.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not AtmosSource", source instanceof AtmosSource);
        AtmosSource atmosSource = (AtmosSource) source;

        Object target = sync.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not AtmosTarget", target instanceof AtmosTarget);
        AtmosTarget atmosTarget = (AtmosTarget) target;

        Assert.assertEquals("source sourcePath mismatch", sourcePath, atmosSource.getNamespaceRoot());
        Assert.assertEquals("source sourceOidList mismatch", sourceOidList, atmosSource.getOidFile());
        Assert.assertEquals("source sourceNameList mismatch", sourceNameList, atmosSource.getNameFile());
        Assert.assertEquals("source sourceSqlQuery mismatch", sourceSqlQuery, atmosSource.getQuery());
        Assert.assertNotNull("source DataSource is null", atmosSource.getDataSource());
        Assert.assertTrue("source deleteTags should be enabled", atmosSource.isDeleteTags());
        Assert.assertEquals("target targetPath mismatch", targetPath, atmosTarget.getDestNamespace());
        Assert.assertEquals("target targetChecksum mismatch", targetChecksum, atmosTarget.getChecksum());
        Assert.assertEquals("target retDelayWindow mismatch", retDelayWindow, "" + atmosTarget.getRetentionDelayWindow());
        Assert.assertTrue("target noUpdate should be enabled", atmosTarget.isNoUpdate());
    }

    private void testAtmosParse(String protocol, String[] hosts, int port, String uid, String secret, String rootPath) {
        String portStr = port >= 0 ? ":" + port : "";
        String uri = String.format("atmos:%s://%s:%s@%s%s%s",
                protocol, uid, secret, join(hosts, ","), portStr, rootPath == null ? "" : rootPath);

        AtmosUtil.AtmosUri atmosUri = AtmosUtil.parseUri(uri);

        for (int i = 0; i < hosts.length; i++) {
            Assert.assertEquals("protocol different", protocol, atmosUri.endpoints.get(i).getScheme());
            Assert.assertEquals("host different", hosts[i], atmosUri.endpoints.get(i).getHost());
            Assert.assertEquals("port different", port, atmosUri.endpoints.get(i).getPort());
        }
        Assert.assertEquals("accessKey different", uid, atmosUri.uid);
        Assert.assertEquals("secretKey different", secret, atmosUri.secret);
        Assert.assertEquals("rootKey different", rootPath, atmosUri.rootPath);
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
                "-target", "dummy",
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

        // use reflection to bootstrap ViPRSync using CLI arguments
        Method optionsMethod = ViPRSync.class.getDeclaredMethod("cliBootstrap", String[].class);
        optionsMethod.setAccessible(true);
        ViPRSync sync = (ViPRSync) optionsMethod.invoke(null, (Object) args);

        Iterator<SyncFilter> filters = sync.getFilters().iterator();

        SyncFilter filter = filters.next();
        Assert.assertTrue("first filter is not encryption", filter instanceof EncryptionFilter);
        EncryptionFilter encFilter = (EncryptionFilter) filter;
        Assert.assertEquals("enc keystore mismatch", encKeystore, encFilter.getKeystoreFile());
        Assert.assertEquals("enc keystorePass mismatch", encKeyPass, encFilter.getKeystorePass());
        Assert.assertEquals("enc keyAlias mismatch", encKeyAlias, encFilter.getKeyAlias());
        Assert.assertTrue("enc forceString mismatch", encFilter.isForceStrong());
        Assert.assertTrue("enc failIfEncrypted mismatch", encFilter.isFailIfEncrypted());
        Assert.assertTrue("enc updateMtime mismatch", encFilter.isUpdateMtime());

        filter = filters.next();
        Assert.assertTrue("second filter is not decryption", filter instanceof DecryptionFilter);
        DecryptionFilter decFilter = (DecryptionFilter) filter;
        Assert.assertEquals("dec keystore mismatch", decKeystore, decFilter.getKeystoreFile());
        Assert.assertEquals("dec keystorePass mismatch", decKeyPass, decFilter.getKeystorePass());
        Assert.assertTrue("dec failIfNotEncrypted mismatch", decFilter.isFailIfNotEncrypted());
        Assert.assertTrue("dec updateMtime mismatch", decFilter.isUpdateMtime());
    }

    @Test
    public void testCommonCli() throws Exception {
        String sourcePath = "/source/path";
        String targetPath = "/target/path";

        int bufferSize = 123456;

        String[] args = new String[]{
                "-source", "atmos:http://ace5d3da351242bcb095eb841ad40371/test:HkayrXoENUQ3VCMCaaViS0tbpDs=@10.6.143.97,10.6.143.98,10.6.143.99,10.6.143.100" + sourcePath,
                "-target", "atmos:https://wuser1@SANITY.LOCAL:awNGq7jVFDm3ZLcvVdY0kNKjs96/FX1I1iJJ+fqi@10.249.237.105,10.249.237.106:9023" + targetPath,
                "--metadata-only",
                "--ignore-metadata",
                "--include-acl",
                "--ignore-invalid-acls",
                "--include-retention-expiration",
                "--force",
                "--io-buffer-size", "" + bufferSize
        };

        // use reflection to bootstrap ViPRSync using CLI arguments
        Method optionsMethod = ViPRSync.class.getDeclaredMethod("cliBootstrap", String[].class);
        optionsMethod.setAccessible(true);
        ViPRSync sync = (ViPRSync) optionsMethod.invoke(null, (Object) args);

        SyncSource<?> source = sync.getSource();
        Assert.assertNotNull("source is null", source);
        Assert.assertTrue("source is not AtmosSource", source instanceof AtmosSource);

        SyncTarget target = sync.getTarget();
        Assert.assertNotNull("target is null", target);
        Assert.assertTrue("target is not AtmosTarget", target instanceof AtmosTarget);

        Assert.assertTrue("source metadataOnly mismatch", source.isMetadataOnly());
        Assert.assertTrue("source ignoreMetadata mismatch", source.isIgnoreMetadata());
        Assert.assertTrue("source includeAcl mismatch", source.isIncludeAcl());
        Assert.assertTrue("source ignoreInvalidAcls mismatch", source.isIgnoreInvalidAcls());
        Assert.assertTrue("source includeRetentionExpiration mismatch", source.isIncludeRetentionExpiration());
        Assert.assertTrue("source force mismatch", source.isForce());
        Assert.assertEquals("source bufferSize mismatch", bufferSize, source.getBufferSize());
        Assert.assertTrue("target metadataOnly mismatch", target.isMetadataOnly());
        Assert.assertTrue("target ignoreMetadata mismatch", target.isIgnoreMetadata());
        Assert.assertTrue("target includeAcl mismatch", target.isIncludeAcl());
        Assert.assertTrue("target ignoreInvalidAcls mismatch", target.isIgnoreInvalidAcls());
        Assert.assertTrue("target includeRetentionExpiration mismatch", target.isIncludeRetentionExpiration());
        Assert.assertTrue("target force mismatch", target.isForce());
        Assert.assertEquals("target bufferSize mismatch", bufferSize, target.getBufferSize());
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
