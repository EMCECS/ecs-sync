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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.Calendar;
import java.util.Date;

public class SqliteDbServiceTest {
    private static final String DB_FILE = ":memory:";

    protected AbstractDbService dbService;

    @Before
    public void setup() throws Exception {
        dbService = new SqliteDbService(DB_FILE);
    }

    @After
    public void teardown() throws Exception {
        if (dbService != null) dbService.close();
    }

    @Test
    public void testRowInsert() throws Exception {
        // test with various parameters and verify result
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0); // truncate ms since DB doesn't store it
        Date now = cal.getTime();
        byte[] data = "Hello World!".getBytes("UTF-8");
        SyncStorage storage = new TestStorage();

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
        SyncObject object = new SyncObject(storage, id, new ObjectMetadata().withDirectory(true));
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

        id = "4";
        object = new SyncObject(storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Transferred);
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

        id = "5";
        object = new SyncObject(storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.InVerification);
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

        id = "6";
        object = new SyncObject(storage, id, new ObjectMetadata().withContentLength(data.length));
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

        id = "7";
        object = new SyncObject(storage, id, new ObjectMetadata().withContentLength(data.length));
        context = new ObjectContext().withSourceSummary(new ObjectSummary(id, false, data.length)).withObject(object).withOptions(new SyncOptions());
        context.setStatus(ObjectStatus.Error);
        dbService.setStatus(context, "blah", true);
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
        Assert.assertEquals("blah", rowSet.getString("error_message"));
    }

    @Test
    public void testRowUpdate() throws Exception {
        byte[] data = "Hello World!".getBytes("UTF-8");
        String id = "1";
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MILLISECOND, 0); // truncate ms since DB doesn't store it
        Date now = cal.getTime();

        SyncObject object = new SyncObject(new TestStorage(), id, new ObjectMetadata().withContentLength(data.length));
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

        String error = "ouch";
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

        context.setStatus(ObjectStatus.Transferred);
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
    }

    protected long getUnixTime(SqlRowSet rowSet, String field) {
        return rowSet.getLong(field);
    }

    private SqlRowSet getRowSet(String id) {
        JdbcTemplate jdbcTemplate = dbService.getJdbcTemplate();
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + dbService.getObjectsTableName() + " WHERE source_id=?", id);
        rowSet.next();
        return rowSet;
    }
}
