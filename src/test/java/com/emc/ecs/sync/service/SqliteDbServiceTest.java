/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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

import com.emc.ecs.sync.model.ObjectStatus;
import com.emc.ecs.sync.test.TestSyncObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.ArrayList;
import java.util.Date;

public class SqliteDbServiceTest {
    private static final String DB_FILE = ":memory:";

    DbService dbService;

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
        Date now = new Date();
        byte[] data = "Hello World!".getBytes("UTF-8");

        String id = "1";
        dbService.setStatus(new TestSyncObject(null, id, id, new byte[]{}, null), ObjectStatus.InTransfer, null, true);
        SqlRowSet rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(0, rowSet.getInt("size"));
        Assert.assertEquals(0, rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));

        // double check that dates are represented accurately
        // the transfer_start date should be less than a second later than the start of this method
        Assert.assertTrue(rowSet.getLong("transfer_start") - now.getTime() < 1000);

        try {
            dbService.setStatus(new TestSyncObject(null, "2", "2", new byte[]{}, null), null, null, true);
            Assert.fail("status should be required");
        } catch (NullPointerException e) {
            // expected
        }

        id = "3";
        TestSyncObject object = new TestSyncObject(null, id, id, null, new ArrayList<TestSyncObject>());
        object.getMetadata().setModificationTime(now);
        object.incFailureCount();
        dbService.setStatus(object, ObjectStatus.Verified, "foo", true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertTrue(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(0, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertNotEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals("foo", rowSet.getString("error_message"));

        id = "4";
        object = new TestSyncObject(null, id, id, data, null);
        dbService.setStatus(object, ObjectStatus.Transferred, null, true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.Transferred.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));

        id = "5";
        object = new TestSyncObject(null, id, id, data, null);
        dbService.setStatus(object, ObjectStatus.InVerification, null, true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.InVerification.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertNotEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));

        id = "6";
        object = new TestSyncObject(null, id, id, data, null);
        dbService.setStatus(object, ObjectStatus.RetryQueue, "blah", true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.RetryQueue.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertEquals("blah", rowSet.getString("error_message"));

        id = "7";
        object = new TestSyncObject(null, id, id, data, null);
        dbService.setStatus(object, ObjectStatus.Error, "blah", true);
        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertNull(rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(0, rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.Error.getValue(), rowSet.getString("status"));
        Assert.assertEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertEquals("blah", rowSet.getString("error_message"));
    }

    @Test
    public void testRowUpdate() throws Exception {
        byte[] data = "Hello World!".getBytes("UTF-8");
        String id = "1";
        Date now = new Date();
        TestSyncObject object = new TestSyncObject(null, id, id, data, null);
        object.setTargetIdentifier(id);
        object.getMetadata().setModificationTime(now);

        dbService.setStatus(object, ObjectStatus.InTransfer, null, true);

        SqlRowSet rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertNull(rowSet.getString("error_message"));

        String error = "ouch";
        dbService.setStatus(object, ObjectStatus.RetryQueue, error, false);
        object.incFailureCount();

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.RetryQueue.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(0, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));

        dbService.setStatus(object, ObjectStatus.InTransfer, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.InTransfer.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));

        dbService.setStatus(object, ObjectStatus.Transferred, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.Transferred.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));

        dbService.setStatus(object, ObjectStatus.InVerification, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.InVerification.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertNotEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));

        dbService.setStatus(object, ObjectStatus.Verified, null, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertNotEquals(0, rowSet.getLong("verify_start"));
        Assert.assertNotEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));

        dbService.setStatus(object, ObjectStatus.Error, error, false);

        rowSet = getRowSet(id);
        Assert.assertEquals(id, rowSet.getString("source_id"));
        Assert.assertEquals(id, rowSet.getString("target_id"));
        Assert.assertFalse(rowSet.getBoolean("is_directory"));
        Assert.assertEquals(data.length, rowSet.getInt("size"));
        Assert.assertEquals(now.getTime(), rowSet.getLong("mtime"));
        Assert.assertEquals(ObjectStatus.Error.getValue(), rowSet.getString("status"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_start"));
        Assert.assertNotEquals(0, rowSet.getLong("transfer_complete"));
        Assert.assertNotEquals(0, rowSet.getLong("verify_start"));
        Assert.assertEquals(0, rowSet.getLong("verify_complete"));
        Assert.assertEquals(1, rowSet.getInt("retry_count"));
        Assert.assertEquals(error, rowSet.getString("error_message"));
    }

    private SqlRowSet getRowSet(String id) {
        JdbcTemplate jdbcTemplate = dbService.getJdbcTemplate();
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet("SELECT * FROM " + dbService.getObjectsTableName() + " WHERE source_id=?", id);
        rowSet.next();
        return rowSet;
    }
}
