/*
 * Copyright (c) 2017-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.test.TestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class MySQLExtendedDbServiceTest extends SqliteExtendedDbServiceTest {
    @BeforeEach
    public void setup() throws Exception {
        String mysqlConnectString = TestConfig.getProperties().getProperty(TestConfig.PROP_MYSQL_CONNECT_STRING);
        String mysqlEncPassword = TestConfig.getProperties().getProperty(TestConfig.PROP_MYSQL_ENC_PASSWORD);
        Assumptions.assumeTrue(mysqlConnectString != null);
        dbService = new MySQLDbService(mysqlConnectString, null, null, mysqlEncPassword, true);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (dbService != null) {
            dbService.deleteDatabase();
            dbService.close();
        }
    }

    @Override
    protected long getUnixTime(SqlRowSet rowSet, String field) {
        // Unable to call rowSet.getTimestamp(field) due to incompatibility introduced by the Date-Time Types change in mysql:mysql-connector-java:8.x
        LocalDateTime localDateTime = (LocalDateTime) rowSet.getObject(field);
        if (localDateTime == null) return 0;
        else return Timestamp.valueOf(localDateTime).getTime();
    }
}
