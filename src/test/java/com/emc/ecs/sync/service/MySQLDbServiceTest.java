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

import com.emc.ecs.sync.test.TestConfig;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.Timestamp;

public class MySQLDbServiceTest extends SqliteDbServiceTest {
    @Before
    public void setup() throws Exception {
        String mysqlConnectString = TestConfig.getProperties().getProperty(TestConfig.PROP_MYSQL_CONNECT_STRING);
        String mysqlEncPassword = TestConfig.getProperties().getProperty(TestConfig.PROP_MYSQL_ENC_PASSWORD);
        Assume.assumeNotNull(mysqlConnectString);
        dbService = new MySQLDbService(mysqlConnectString, null, null, mysqlEncPassword);
    }

    @After
    public void teardown() throws Exception {
        if (dbService != null) {
            dbService.deleteDatabase();
            dbService.close();
        }
    }

    @Override
    protected long getUnixTime(SqlRowSet rowSet, String field) {
        Timestamp timestamp = rowSet.getTimestamp(field);
        if (timestamp == null) return 0;
        else return timestamp.getTime();
    }
}
