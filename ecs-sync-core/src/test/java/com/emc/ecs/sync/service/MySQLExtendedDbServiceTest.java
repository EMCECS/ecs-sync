package com.emc.ecs.sync.service;

import com.emc.ecs.sync.test.TestConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.Timestamp;

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
        Timestamp timestamp = rowSet.getTimestamp(field);
        if (timestamp == null) return 0;
        else return timestamp.getTime();
    }
}
