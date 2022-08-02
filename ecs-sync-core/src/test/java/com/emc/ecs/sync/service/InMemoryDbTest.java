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

public class InMemoryDbTest {
    // no shared cache, so each instance should be a completely new in-memory DB
    @Test
    public void testTwoTables() {
        String table1 = "table1", table2 = "table2";
        String table1Insert = String.format("insert into %s (%s, %s, %s, %s) values (?,?,?,?)",
                table1, SOURCE_ID, TARGET_ID, IS_DIRECTORY, STATUS);
        String table2Insert = String.format("insert into %s (%s, %s, %s, %s) values (?,?,?,?)",
                table2, SOURCE_ID, TARGET_ID, IS_DIRECTORY, STATUS);

        InMemoryDbService dbService = new InMemoryDbService(false);
        dbService.setObjectsTableName(table1);
        // creates table1
        dbService.initCheck();

        // insert two records
        dbService.getJdbcTemplate().update(table1Insert, "foo", "foo", "false", "dummy_status");
        dbService.getJdbcTemplate().update(table1Insert, "bar", "bar", "false", "dummy_status");

        dbService.close();

        // new reference should be completely blank
        dbService = new InMemoryDbService(false);
        dbService.setObjectsTableName(table2);
        dbService.initCheck();

        // verify empty table
        Assertions.assertEquals(0, dbService.getJdbcTemplate().queryForObject("select count(*) from " + table2, Long.class).longValue());

        // insert one overlapping record and one unique record
        dbService.getJdbcTemplate().update(table2Insert, "bar", "bar", "false", "dummy_status");
        dbService.getJdbcTemplate().update(table2Insert, "baz", "baz", "false", "dummy_status");

        // verify only 2 rows
        Assertions.assertEquals(2, dbService.getJdbcTemplate().queryForObject("select count(*) from " + table2, Long.class).longValue());

        dbService.close();
    }
}
