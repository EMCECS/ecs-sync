/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.emc.ecs.sync.service.SyncRecordHandler.*;

public class SyncRecordHandlerTest {
    @Test
    public void testInsert() {
        String query = "insert into foo (source_id,target_id,is_directory,size,mtime,status,transfer_start,transfer_complete,verify_start,verify_complete,retry_count,error_message,is_source_deleted) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Assertions.assertEquals(query, SyncRecordHandler.insert("foo", null));

        query = "insert into foo (source_id,target_id,is_directory,size,mtime,status,transfer_start,retry_count) values (?, ?, ?, ?, ?, ?, ?, ?)";
        DbParams params = DbParams.create()
                .addDataParam(SOURCE_ID, null)
                .addDataParam(TARGET_ID, null)
                .addDataParam(IS_DIRECTORY, null)
                .addDataParam(SIZE, null)
                .addDataParam(MTIME, null)
                .addDataParam(STATUS, null)
                .addDataParam(TRANSFER_START, null)
                .addDataParam(RETRY_COUNT, null);
        Assertions.assertEquals(query, SyncRecordHandler.insert("foo", params));
    }

    @Test
    public void testSelectBySourceId() {
        NoDbService dbService = new NoDbService(false);
        SyncRecordHandler recordHandler = new SyncRecordHandler(dbService.getMaxErrorSize(), dbService);
        String query = "select source_id,target_id,is_directory,size,mtime,status,transfer_start,transfer_complete,verify_start,verify_complete,retry_count,error_message,is_source_deleted from foo where source_id = ?";
        Assertions.assertEquals(query, recordHandler.selectBySourceId("foo"));
    }

    @Test
    public void testUpdateBySourceId() {
        String query = "update foo set target_id=?, is_directory=?, size=?, mtime=?, status=?, transfer_start=?, transfer_complete=?, verify_start=?, verify_complete=?, retry_count=?, error_message=?, is_source_deleted=? where source_id = ?";
        Assertions.assertEquals(query, SyncRecordHandler.updateBySourceId("foo", null));

        DbParams params = DbParams.create()
                .addDataParam(TARGET_ID, null)
                .addDataParam(IS_DIRECTORY, null)
                .addDataParam(SIZE, null)
                .addDataParam(MTIME, null)
                .addDataParam(STATUS, null)
                .addDataParam(TRANSFER_START, null)
                .addDataParam(RETRY_COUNT, null);
        query = "update foo set target_id=?, is_directory=?, size=?, mtime=?, status=?, transfer_start=?, retry_count=? where source_id = ?";
        Assertions.assertEquals(query, SyncRecordHandler.updateBySourceId("foo", params));
    }
}
