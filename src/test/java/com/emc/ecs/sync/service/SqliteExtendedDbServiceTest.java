package com.emc.ecs.sync.service;

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.TestStorage;
import org.apache.commons.compress.utils.Charsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class SqliteExtendedDbServiceTest extends SqliteDbServiceTest {
    @Before
    public void setup() throws Exception {
        dbService = new SqliteDbService(IN_MEMORY_JDBC_URL, true);
    }

    @Test
    public void testRowInsert() throws Exception {
        testRowInsert(SyncObject.class, false);
    }

    @Test
    public void testRowInsertLongMD5() throws Exception {
        testRowInsert(LongMD5Object.class, true);
    }

    private <T extends SyncObject> void testRowInsert(Class<T> objectClass, boolean nullMd5) throws Exception {
        // test with various parameters and verify result
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0); // truncate ms since DB doesn't store it
        Date now = cal.getTime();
        byte[] data = "Hello World!".getBytes(StandardCharsets.UTF_8);
        SyncStorage<?> storage = new TestStorage();

        String id = "1";
        ObjectContext context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, 0)).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.InTransfer);
        dbService.setStatus(context, null, true);
        SqlRowSet rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(0, rowSet.getInt("size"));
        Assert.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));
        Assert.assertNull(rowSet.getString("source_retention_end_time"));
        Assert.assertNull(rowSet.getString("target_mtime"));
        Assert.assertNull(rowSet.getString("target_md5"));
        Assert.assertNull(rowSet.getString("target_retention_end_time"));
        Assert.assertNull(rowSet.getString("first_error_message"));

        // double check that dates are represented accurately
        // the transfer_start date should be less than a second later than the start of this method
        Assert.assertTrue(getUnixTime(rowSet, "transfer_start") - now.getTime() < 1000);

        try {
            context = new ObjectContext().withSourceSummary(new ObjectSummary("2", false, 0));
            dbService.setStatus(context, null, true);
            Assert.fail("status should be required");
        } catch (NullPointerException e) {
            // expected
        }

        id = "3";
        SyncObject object = createSyncObject(objectClass, storage, id, new ObjectMetadata().withDirectory(true));
        object.getMetadata().setModificationTime(now);
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, true, 0)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Verified);
        context.incFailures();
        dbService.setStatus(context, "foo", true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertTrue(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(0, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals("foo", rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));
        Assert.assertNull(rowSet.getString("source_retention_end_time"));
        Assert.assertNull(rowSet.getString("target_mtime"));
        Assert.assertNull(rowSet.getString("target_md5"));
        Assert.assertNull(rowSet.getString("target_retention_end_time"));
        Assert.assertEquals("foo", rowSet.getString("first_error_message"));

        id = "4";
        object = createSyncObject(objectClass, storage, id, new ObjectMetadata().withContentLength(data.length));
        object.setDataStream(new ByteArrayInputStream("foo".getBytes(Charsets.UTF_8)));
        object.getMd5Hex(true); // make sure MD5 is recorded
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Transferred);
        context.setTargetMtime(new Date());
        dbService.setStatus(context, null, true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.Transferred.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        if (nullMd5) Assert.assertNull(rowSet.getString("source_md5"));
        else Assert.assertEquals("ACBD18DB4CC2F85CEDEF654FCCC4A4D8", rowSet.getString("source_md5"));
        Assert.assertNull(rowSet.getString("source_retention_end_time"));
        Assert.assertEquals(context.getTargetMtime().getTime() / 1000, getUnixTime(rowSet, "target_mtime") / 1000);
        Assert.assertNull(rowSet.getString("target_md5"));
        Assert.assertNull(rowSet.getString("target_retention_end_time"));
        Assert.assertNull(rowSet.getString("first_error_message"));

        id = "5";
        object = createSyncObject(objectClass, storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.InVerification);
        object.getMetadata().setRetentionEndDate(new Date(System.currentTimeMillis() + 4600000));
        context.setTargetMtime(new Date());
        context.setTargetMd5("d41d8cd98f00b204e9800998ecf8427e");
        context.setTargetRetentionEndTime(new Date(System.currentTimeMillis() + 3600000));
        dbService.setStatus(context, null, true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.InVerification.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));
        Assert.assertEquals(object.getMetadata().getRetentionEndDate().getTime() / 1000,
                getUnixTime(rowSet, "source_retention_end_time") / 1000);
        Assert.assertEquals(context.getTargetMtime().getTime() / 1000, getUnixTime(rowSet, "target_mtime") / 1000);
        Assert.assertEquals(context.getTargetMd5(), rowSet.getString("target_md5"));
        Assert.assertEquals(context.getTargetRetentionEndTime().getTime() / 1000,
                getUnixTime(rowSet, "target_retention_end_time") / 1000);

        id = "6";
        object = createSyncObject(objectClass, storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.RetryQueue);
        dbService.setStatus(context, "blah", true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.RetryQueue.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertEquals("blah", rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));
        Assert.assertEquals("blah", rowSet.getString("first_error_message"));

        id = "7";
        object = createSyncObject(objectClass, storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Error);
        String error = "foo'bar \u00a1\u00bf !@#$%^&*()-_=+ 查找的"; // make sure we can handle quotes and extended chars
        dbService.setStatus(context, error, true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.Error.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));
        Assert.assertEquals(error, rowSet.getString("first_error_message"));
    }

    @Test
    public void testRowUpdate() throws Exception {
        testRowUpdate(SyncObject.class, false);
    }

    @Test
    public void testRowUpdateLongMD5() throws Exception {
        testRowUpdate(LongMD5Object.class, true);
    }

    private <T extends SyncObject> void testRowUpdate(Class<T> objectClass, boolean nullMd5) throws Exception {
        byte[] data = "Hello World!".getBytes(StandardCharsets.UTF_8);
        String id = "1";
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0); // truncate ms since DB doesn't store it
        Date now = cal.getTime();

        SyncObject object = createSyncObject(objectClass, new TestStorage(), id, new ObjectMetadata().withContentLength(data.length));
        object.getMetadata().setModificationTime(now);

        ObjectContext context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object);
        context.setStatus(ObjectStatus.InTransfer);
        context.setTargetId(id);
        context.setOptions(new SyncOptions());

        dbService.setStatus(context, null, true);

        SqlRowSet rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));
        Assert.assertNull(rowSet.getString("source_retention_end_time"));
        Assert.assertNull(rowSet.getString("target_mtime"));
        Assert.assertNull(rowSet.getString("target_md5"));
        Assert.assertNull(rowSet.getString("target_retention_end_time"));
        Assert.assertNull(rowSet.getString("first_error_message"));

        String error = "foo'bar \u00a1\u00bf !@#$%^&*()-_=+ 查找的"; // make sure we can handle quotes and extended chars
        String firstError = error;
        context.setStatus(ObjectStatus.RetryQueue);
        dbService.setStatus(context, error, false);
        context.incFailures();

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.RetryQueue.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));
        Assert.assertEquals(error, rowSet.getString("first_error_message"));

        context.setStatus(ObjectStatus.InTransfer);
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        Assert.assertNull(rowSet.getString("source_md5"));

        context.setStatus(ObjectStatus.Transferred);
        object.setDataStream(new ByteArrayInputStream("foo".getBytes(Charsets.UTF_8)));
        object.getMd5Hex(true); // make sure MD5 is recorded
        object.getMetadata().setRetentionEndDate(new Date(System.currentTimeMillis() + 4600000));
        context.setTargetMtime(new Date());
        context.setTargetMd5("d41d8cd98f00b204e9800998ecf8427e");
        context.setTargetRetentionEndTime(new Date(System.currentTimeMillis() + 3600000));
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.Transferred.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        if (nullMd5) Assert.assertNull(rowSet.getString("source_md5"));
        else Assert.assertEquals("ACBD18DB4CC2F85CEDEF654FCCC4A4D8", rowSet.getString("source_md5"));
        Assert.assertEquals(object.getMetadata().getRetentionEndDate().getTime() / 1000,
                getUnixTime(rowSet, "source_retention_end_time") / 1000);
        Assert.assertEquals(context.getTargetMtime().getTime() / 1000,
                getUnixTime(rowSet, "target_mtime") / 1000);
        Assert.assertEquals(context.getTargetMd5(), rowSet.getString("target_md5"));
        Assert.assertEquals(context.getTargetRetentionEndTime().getTime() / 1000,
                getUnixTime(rowSet, "target_retention_end_time") / 1000);

        context.setStatus(ObjectStatus.InVerification);
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.InVerification.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        if (nullMd5) Assert.assertNull(rowSet.getString("source_md5"));
        else Assert.assertEquals("ACBD18DB4CC2F85CEDEF654FCCC4A4D8", rowSet.getString("source_md5"));

        context.setStatus(ObjectStatus.Verified);
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        if (nullMd5) Assert.assertNull(rowSet.getString("source_md5"));
        else Assert.assertEquals("ACBD18DB4CC2F85CEDEF654FCCC4A4D8", rowSet.getString("source_md5"));

        error = "foobar";
        context.setStatus(ObjectStatus.Error);
        dbService.setStatus(context, error, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assert.assertEquals(ObjectStatus.Error.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assert.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assert.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
        Assert.assertFalse(rowSet.getBoolean("is_source_deleted"));
        if (nullMd5) Assert.assertNull(rowSet.getString("source_md5"));
        else Assert.assertEquals("ACBD18DB4CC2F85CEDEF654FCCC4A4D8", rowSet.getString("source_md5"));
        Assert.assertEquals(firstError, rowSet.getString("first_error_message"));
    }

    @Test
    public void testErrorList() throws Exception {
        byte[] data = "Hello World!".getBytes(StandardCharsets.UTF_8);
        String id = "1";
        String wrongMd5 = "feefoofum";
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0); // truncate ms since DB doesn't store it
        Date now = cal.getTime();
        cal.add(Calendar.YEAR, 1);
        Date targetNow = cal.getTime();

        SyncObject object = createSyncObject(SyncObject.class, new TestStorage(), id, new ObjectMetadata().withContentLength(data.length));
        object.getMetadata().setModificationTime(now);
        object.setDataStream(new ByteArrayInputStream(data));
        object.getMd5Hex(true); // make sure MD5 is available

        ObjectContext context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object);
        context.setStatus(ObjectStatus.Transferred);
        context.setTargetId(id);
        context.setTargetMd5(wrongMd5);
        context.setTargetMtime(targetNow);
        context.setOptions(new SyncOptions());

        // setting status to Transferred first will make sure source MD5 is set
        dbService.setStatus(context, null, true);

        context.setStatus(ObjectStatus.Error);
        context.incFailures();
        String error = "foo'bar \u00a1\u00bf !@#$%^&*()-_=+ 查找的"; // make sure we can handle quotes and extended chars

        dbService.setStatus(context, error, false);

        // make sure ExtendedSyncRecord comes back
        final AtomicInteger count = new AtomicInteger();
        dbService.getSyncErrors().forEach(record -> {
            Assert.assertEquals(ExtendedSyncRecord.class, record.getClass());
            count.incrementAndGet();
            ExtendedSyncRecord eRecord = (ExtendedSyncRecord) record;
            Assert.assertEquals(id, record.getSourceId());
            Assert.assertEquals(id, record.getTargetId());
            Assert.assertFalse(record.isDirectory());
            Assert.assertEquals(data.length, record.getSize());
            Assert.assertEquals(now, record.getMtime());
            Assert.assertEquals(ObjectStatus.Error, record.getStatus());
            Assert.assertNull(record.getTransferStart());
            Assert.assertNotNull(record.getTransferComplete());
            Assert.assertNull(record.getVerifyStart());
            Assert.assertNull(record.getVerifyComplete());
            Assert.assertEquals(1, record.getRetryCount());
            Assert.assertEquals(error, record.getErrorMessage());
            Assert.assertFalse(record.isSourceDeleted());
            Assert.assertEquals(object.getMd5Hex(false), eRecord.getSourceMd5());
            Assert.assertNull(eRecord.getSourceRetentionEndTime());
            Assert.assertEquals(targetNow, eRecord.getTargetMtime());
            Assert.assertEquals(wrongMd5, eRecord.getTargetMd5());
            Assert.assertNull(eRecord.getTargetRetentionEndTime());
            Assert.assertEquals(error, eRecord.getFirstErrorMessage());
        });
        // only 1 row
        Assert.assertEquals(1, count.get());
    }

    static class LongMD5Object extends SyncObject {
        public LongMD5Object(SyncStorage<?> source, String relativePath, ObjectMetadata metadata) {
            super(source, relativePath, metadata);
        }

        @Override
        public String getMd5Hex(boolean forceRead) {
            String md5Hex = super.getMd5Hex(forceRead);
            return "this-is-a-super-long-md5-hex-value-that-will-not-fit-into-32-characters-{" + md5Hex + "}" +
                    "-block0{" + md5Hex + "}-block1{" + md5Hex + "}-block2{" + md5Hex + "}-block3{" + md5Hex + "}";
        }
    }
}
