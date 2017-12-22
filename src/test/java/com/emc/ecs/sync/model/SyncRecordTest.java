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
package com.emc.ecs.sync.model;

import com.emc.ecs.sync.service.SyncRecord;
import org.junit.Assert;
import org.junit.Test;

public class SyncRecordTest {
    @Test
    public void testInsert() {
        String query = "insert into foo (source_id,target_id,is_directory,size,mtime,status,transfer_start,transfer_complete,verify_start,verify_complete,retry_count,error_message,is_source_deleted) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Assert.assertEquals(query, SyncRecord.insert("foo"));

        query = "insert into foo (source_id,target_id,is_directory,size,mtime,status,transfer_start,retry_count) values (?, ?, ?, ?, ?, ?, ?, ?)";
        Assert.assertEquals(query, SyncRecord.insert("foo", "source_id", "target_id", "is_directory", "size", "mtime", "status", "transfer_start", "retry_count"));
    }

    @Test
    public void testSelectBySourceId() {
        String query = "select source_id,target_id,is_directory,size,mtime,status,transfer_start,transfer_complete,verify_start,verify_complete,retry_count,error_message,is_source_deleted from foo where source_id = ?";
        Assert.assertEquals(query, SyncRecord.selectBySourceId("foo"));
    }

    @Test
    public void testUpdateBySourceId() {
        String query = "update foo set target_id=?, is_directory=?, size=?, mtime=?, status=?, transfer_start=?, transfer_complete=?, verify_start=?, verify_complete=?, retry_count=?, error_message=?, is_source_deleted=? where source_id = ?";
        Assert.assertEquals(query, SyncRecord.updateBySourceId("foo"));

        query = "update foo set target_id=?, is_directory=?, size=?, mtime=?, status=?, transfer_start=?, retry_count=? where source_id = ?";
        Assert.assertEquals(query, SyncRecord.updateBySourceId("foo", "target_id", "is_directory", "size", "mtime", "status", "transfer_start", "retry_count"));
    }
}
