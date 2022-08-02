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

import com.emc.ecs.sync.test.TestConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class MySQLAddExtendedFieldsTest {
    private static final Logger log = LoggerFactory.getLogger(MySQLAddExtendedFieldsTest.class);

    private String mysqlConnectString;
    private String mysqlEncPassword;
    private String tableName = "ecs_sync_add_extended_fields_test";

    @BeforeEach
    public void setup() throws Exception {
        mysqlConnectString = TestConfig.getProperties().getProperty(TestConfig.PROP_MYSQL_CONNECT_STRING);
        mysqlEncPassword = TestConfig.getProperties().getProperty(TestConfig.PROP_MYSQL_ENC_PASSWORD);
        Assumptions.assumeTrue(mysqlConnectString != null);
    }

    @Test
    public void testAddingAllExtendedFields() {
        AbstractDbService dbService = new MySQLDbService(mysqlConnectString, null, null, mysqlEncPassword, false);

        try {
            // initialize basic table
            dbService.setObjectsTableName(tableName);
            dbService.getAllRecords();

            // check only basic fields are present
            // the following query will never return any rows, but will contain metadata to identify the table columns
            List<DbField> dbFields = dbService.getJdbcTemplate().query("SELECT * FROM " + dbService.getObjectsTableName() + " WHERE 1=0",
                    rs -> {
                        List<DbField> fields = new ArrayList<>();
                        ResultSetMetaData rsmd = rs.getMetaData();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            String colName = rsmd.getColumnName(i);
                            Optional<DbField> field = SyncRecordHandler.ALL_FIELDS.stream()
                                    .filter(f -> f.name().equalsIgnoreCase(colName))
                                    .findFirst();
                            Assertions.assertTrue(field.isPresent(), "Unknown DB column: " + colName);
                            field.ifPresent(fields::add); // add the DbField definition for this column
                        }
                        return fields;
                    });

            // sort lists for comparison
            List<DbField> expectedFields = new ArrayList<>(SyncRecordHandler.ALL_FIELDS);
            expectedFields.sort(Comparator.comparing(DbField::name));
            dbFields.sort(Comparator.comparing(DbField::name));

            Assertions.assertEquals(expectedFields, dbFields);

            dbService.close();

            // add columns to existing table
            dbService = new MySQLDbService(mysqlConnectString, null, null, mysqlEncPassword, true);
            dbService.setObjectsTableName(tableName);
            dbService.getAllRecords();

            // check extended fields are present
            // the following query will never return any rows, but will contain metadata to identify the table columns
            dbFields = dbService.getJdbcTemplate().query("SELECT * FROM " + dbService.getObjectsTableName() + " WHERE 1=0",
                    rs -> {
                        List<DbField> fields = new ArrayList<>();
                        ResultSetMetaData rsmd = rs.getMetaData();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            String colName = rsmd.getColumnName(i);
                            Optional<DbField> field = ExtendedSyncRecordHandler.ALL_FIELDS.stream()
                                    .filter(f -> f.name().equalsIgnoreCase(colName))
                                    .findFirst();
                            Assertions.assertTrue(field.isPresent(), "Unknown DB column: " + colName);
                            field.ifPresent(fields::add); // add the DbField definition for this column
                        }
                        return fields;
                    });

            // sort lists for comparison
            expectedFields = new ArrayList<>(ExtendedSyncRecordHandler.ALL_FIELDS);
            expectedFields.sort(Comparator.comparing(DbField::name));
            dbFields.sort(Comparator.comparing(DbField::name));

            Assertions.assertEquals(expectedFields, dbFields);
        } finally {
            try {
                dbService.deleteDatabase();
            } catch (Exception e) {
                log.warn("could not drop database", e);
            }
            dbService.close();
        }
    }
}
