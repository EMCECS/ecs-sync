package com.emc.ecs.sync.service;

import org.springframework.jdbc.core.JdbcTemplate;

public class InMemoryDbService extends SqliteDbService {
    public InMemoryDbService(boolean extendedFieldsEnabled) {
        super("jdbc:sqlite::memory:", extendedFieldsEnabled);
    }

    @Override
    public void initCheck() {
        super.initCheck();
    }

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return super.getJdbcTemplate();
    }

    @Override
    public void deleteDatabase() {
        getJdbcTemplate().update("DROP TABLE IF EXISTS " + getObjectsTableName());
        super.deleteDatabase();
    }
}
