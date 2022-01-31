/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class SqliteDbServiceTest {
    static final String IN_MEMORY_JDBC_URL = "jdbc:sqlite::memory:";

    protected AbstractDbService dbService;

    @BeforeEach
    public void setup() throws Exception {
        dbService = new SqliteDbService(IN_MEMORY_JDBC_URL, false);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (dbService != null) dbService.close();
    }

    <T extends SyncObject> T createSyncObject(Class<T> objectClass, SyncStorage<?> source, String relativePath, ObjectMetadata metadata)
            throws Exception {
        Constructor<T> constructor = objectClass.getConstructor(SyncStorage.class, String.class, ObjectMetadata.class);
        return constructor.newInstance(source, relativePath, metadata);
    }

    @Test
    public void testRowInsert() throws Exception {
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
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertNull(rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(0, rowSet.getInt("size"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(0, rowSet.getInt("retry_count"));
        Assertions.assertNull(rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        // double check that dates are represented accurately
        // the transfer_start date should be less than a second later than the start of this method
        Assertions.assertTrue(getUnixTime(rowSet, "transfer_start") - now.getTime() < 1000);

        try {
            context = new ObjectContext().withSourceSummary(new ObjectSummary("2", false, 0));
            dbService.setStatus(context, null, true);
            Assertions.fail("status should be required");
        } catch (NullPointerException e) {
            // expected
        }

        id = "3";
        SyncObject object = createSyncObject(SyncObject.class, storage, id, new ObjectMetadata().withDirectory(true));
        object.getMetadata().setModificationTime(now);
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, true, 0)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Verified);
        context.incFailures();
        dbService.setStatus(context, "foo", true);
        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertNull(rowSet.getString("target_id"));
        Assertions.assertTrue(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(0, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(1, rowSet.getInt("retry_count"));
        Assertions.assertEquals("foo", rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        id = "4";
        object = createSyncObject(SyncObject.class, storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Transferred);
        dbService.setStatus(context, null, true);
        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertNull(rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.Transferred.getValue(), rowSet.getString("status"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(0, rowSet.getInt("retry_count"));
        Assertions.assertNull(rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        id = "5";
        object = createSyncObject(SyncObject.class, storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.InVerification);
        dbService.setStatus(context, null, true);
        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertNull(rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.InVerification.getValue(), rowSet.getString("status"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(0, rowSet.getInt("retry_count"));
        Assertions.assertNull(rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        id = "6";
        object = createSyncObject(SyncObject.class, storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.RetryQueue);
        dbService.setStatus(context, "blah", true);
        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertNull(rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.RetryQueue.getValue(), rowSet.getString("status"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(0, rowSet.getInt("retry_count"));
        Assertions.assertEquals("blah", rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        id = "7";
        object = createSyncObject(SyncObject.class, storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Error);
        dbService.setStatus(context, "blah", true);
        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertNull(rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.Error.getValue(), rowSet.getString("status"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(0, rowSet.getInt("retry_count"));
        Assertions.assertEquals("blah", rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));
    }

    @Test
    public void testRowUpdate() throws Exception {
        byte[] data = "Hello World!".getBytes(StandardCharsets.UTF_8);
        String id = "1";
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0); // truncate ms since DB doesn't store it
        Date now = cal.getTime();

        SyncObject object = createSyncObject(SyncObject.class, new TestStorage(), id, new ObjectMetadata().withContentLength(data.length));
        object.getMetadata().setModificationTime(now);

        ObjectContext context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object);
        context.setStatus(ObjectStatus.InTransfer);
        context.setTargetId(id);
        context.setOptions(new SyncOptions());

        dbService.setStatus(context, null, true);

        SqlRowSet rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertEquals(id, rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(0, rowSet.getInt("retry_count"));
        Assertions.assertNull(rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        String error = "ouch";
        context.setStatus(ObjectStatus.RetryQueue);
        dbService.setStatus(context, error, false);
        context.incFailures();

        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertEquals(id, rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.RetryQueue.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(0, rowSet.getInt("retry_count"));
        Assertions.assertEquals(error, rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        context.setStatus(ObjectStatus.InTransfer);
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertEquals(id, rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(1, rowSet.getInt("retry_count"));
        Assertions.assertEquals(error, rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        context.setStatus(ObjectStatus.Transferred);
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertEquals(id, rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.Transferred.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(1, rowSet.getInt("retry_count"));
        Assertions.assertEquals(error, rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        context.setStatus(ObjectStatus.InVerification);
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertEquals(id, rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.InVerification.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(1, rowSet.getInt("retry_count"));
        Assertions.assertEquals(error, rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        context.setStatus(ObjectStatus.Verified);
        dbService.setStatus(context, null, false);

        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertEquals(id, rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(1, rowSet.getInt("retry_count"));
        Assertions.assertEquals(error, rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));

        context.setStatus(ObjectStatus.Error);
        dbService.setStatus(context, error, false);

        rowSet = getRowSet(id);
        Assertions.assertEquals(id, rowSet.getString("source_id"));
        Assertions.assertEquals(id, rowSet.getString("target_id"));
        Assertions.assertFalse(rowSet.getBoolean("is_directory"));
        Assertions.assertEquals(data.length, rowSet.getInt("size"));
        Assertions.assertEquals(now.getTime(), getUnixTime(rowSet, "mtime"));
        Assertions.assertEquals(ObjectStatus.Error.getValue(), rowSet.getString("status"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_start"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "transfer_complete"));
        Assertions.assertNotEquals(0, getUnixTime(rowSet, "verify_start"));
        Assertions.assertEquals(0, getUnixTime(rowSet, "verify_complete"));
        Assertions.assertEquals(1, rowSet.getInt("retry_count"));
        Assertions.assertEquals(error, rowSet.getString("error_message"));
        Assertions.assertFalse(rowSet.getBoolean("is_source_deleted"));
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
            Assertions.assertEquals(SyncRecord.class, record.getClass());
            count.incrementAndGet();
            Assertions.assertEquals(id, record.getSourceId());
            Assertions.assertEquals(id, record.getTargetId());
            Assertions.assertFalse(record.isDirectory());
            Assertions.assertEquals(data.length, record.getSize());
            Assertions.assertEquals(now, record.getMtime());
            Assertions.assertEquals(ObjectStatus.Error, record.getStatus());
            Assertions.assertNull(record.getTransferStart());
            Assertions.assertNotNull(record.getTransferComplete());
            Assertions.assertNull(record.getVerifyStart());
            Assertions.assertNull(record.getVerifyComplete());
            Assertions.assertEquals(1, record.getRetryCount());
            Assertions.assertEquals(error, record.getErrorMessage());
            Assertions.assertFalse(record.isSourceDeleted());
        });
        // only 1 row
        Assertions.assertEquals(1, count.get());
    }

    long getUnixTime(SqlRowSet rowSet, String field) {
        return rowSet.getLong(field);
    }

    SqlRowSet getRowSet(String id) {
        JdbcTemplate jdbcTemplate = dbService.getJdbcTemplate();
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + dbService.getObjectsTableName() + " WHERE source_id=?", id);
        rowSet.next();
        return rowSet;
    }
}
