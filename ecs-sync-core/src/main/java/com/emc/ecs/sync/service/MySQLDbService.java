/*
 * Copyright (c) 2015-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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

import com.emc.ecs.sync.config.SyncOptions;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.MessageDigest;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MySQLDbService extends AbstractDbService {
    private static final Logger log = LoggerFactory.getLogger(MySQLDbService.class);

    private static Key cipherKey;

    private String connectString;
    private String username;
    private String password;
    private volatile boolean closed;

    static {
        try {
            cipherKey = new SecretKeySpec(MessageDigest.getInstance("MD5").digest(SyncOptions.DB_DESC.getBytes()), "AES");
        } catch (GeneralSecurityException e) {
            log.warn("unable to create password cipher key: " + e.toString(), e);
        }
    }

    public static void main(String[] args) {
        if (args.length < 1 || !"encrypt-password".equals(args[0])) {
            printHelp();
            System.exit(1);
        }
        String password = new String(System.console().readPassword("Password to encrypt: "));
        System.out.println("Encrypted password: " + encryptPassword(password));
    }

    public static void printHelp() {
        System.out.println("usage: java -cp ecs-sync-{version}.jar com.emc.ecs.sync.service.MySQLDbService encrypt-password");
    }

    public static String encryptPassword(String password) {
        try {
            Cipher encryptCipher = Cipher.getInstance("AES");
            encryptCipher.init(Cipher.ENCRYPT_MODE, cipherKey);
            return DatatypeConverter.printBase64Binary(encryptCipher.doFinal(password.getBytes()));
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("unable to encrypt password: " + e.toString(), e);
        }
    }

    private static String decryptPassword(String encPassword) {
        try {
            Cipher decryptCipher = Cipher.getInstance("AES");
            decryptCipher.init(Cipher.DECRYPT_MODE, cipherKey);
            return new String(decryptCipher.doFinal(DatatypeConverter.parseBase64Binary(encPassword)));
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("unable to decrypt password: " + e.toString(), e);
        }
    }

    public MySQLDbService(String connectString, String username, String password, String encPassword, boolean extendedFieldsEnabled) {
        super(extendedFieldsEnabled);
        this.connectString = connectString;
        this.username = username;
        this.password = password;
        if (encPassword != null) {
            this.password = decryptPassword(encPassword);
        }
    }

    @Override
    public void deleteDatabase() {
        JdbcTemplate template = createJdbcTemplate();
        try {
            template.execute("drop table if exists " + getObjectsTableName());
        } finally {
            close(template);
        }
    }

    @Override
    public void close() {
        try {
            if (!closed) close(getJdbcTemplate());
        } finally {
            closed = true;
            super.close();
        }
    }

    protected void close(JdbcTemplate template) {
        try {
            ((HikariDataSource) template.getDataSource()).close();
        } catch (RuntimeException e) {
            log.warn("could not close data source", e);
        }
    }

    @Override
    protected JdbcTemplate createJdbcTemplate() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(connectString);
        if (username != null) ds.setUsername(username);
        if (password != null) ds.setPassword(password);
        ds.setMaximumPoolSize(16);
        ds.setMinimumIdle(2);
        ds.addDataSourceProperty("characterEncoding", "utf8");
        ds.addDataSourceProperty("cachePrepStmts", "true");
        ds.addDataSourceProperty("prepStmtCacheSize", "256");
        ds.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds.addDataSourceProperty("defaultFetchSize", "" + Integer.MIN_VALUE);
        return new JdbcTemplate(ds);
    }

    @Override
    protected void createTable() {
        try {
            if (extendedFieldsEnabled) {
                getJdbcTemplate().update("CREATE TABLE IF NOT EXISTS " + getObjectsTableName() + " (" +
                        "source_id VARCHAR(750) PRIMARY KEY NOT NULL," +
                        "target_id VARCHAR(750)," +
                        "is_directory INT NOT NULL," +
                        "size BIGINT," +
                        "mtime DATETIME," +
                        "status VARCHAR(32) NOT NULL," +
                        "transfer_start DATETIME NULL," +
                        "transfer_complete DATETIME NULL," +
                        "verify_start DATETIME NULL," +
                        "verify_complete DATETIME NULL," +
                        "retry_count INT," +
                        "error_message VARCHAR(" + getMaxErrorSize() + ")," +
                        "is_source_deleted INT NULL," +
                        "source_md5 VARCHAR(32) NULL," +
                        "source_retention_end_time DATETIME NULL," +
                        "target_mtime DATETIME NULL," +
                        "target_md5 VARCHAR(32) NULL," +
                        "target_retention_end_time DATETIME NULL," +
                        "first_error_message VARCHAR(" + getMaxErrorSize() + ")," +
                        "INDEX status_idx (status)" +
                        ") ENGINE=InnoDB ROW_FORMAT=COMPRESSED");
            } else {
                getJdbcTemplate().update("CREATE TABLE IF NOT EXISTS " + getObjectsTableName() + " (" +
                        "source_id VARCHAR(750) PRIMARY KEY NOT NULL," +
                        "target_id VARCHAR(750)," +
                        "is_directory INT NOT NULL," +
                        "size BIGINT," +
                        "mtime DATETIME," +
                        "status VARCHAR(32) NOT NULL," +
                        "transfer_start DATETIME NULL," +
                        "transfer_complete DATETIME NULL," +
                        "verify_start DATETIME NULL," +
                        "verify_complete DATETIME NULL," +
                        "retry_count INT," +
                        "error_message VARCHAR(" + getMaxErrorSize() + ")," +
                        "is_source_deleted INT NULL," +
                        "INDEX status_idx (status)" +
                        ") ENGINE=InnoDB ROW_FORMAT=COMPRESSED");
            }
        } catch (RuntimeException e) {
            log.error("could not create DB table {}. note: name may only contain alphanumeric or underscore", getObjectsTableName());
            throw e;
        }

        // add extended fields if necessary
        if (isExtendedFieldsEnabled()) {
            try {
                // find any missing extended fields
                List<DbField> missingFields = getJdbcTemplate().query("SELECT * FROM " + getObjectsTableName() + " WHERE 1=0",
                        rs -> {
                            List<DbField> extendedFields = new ArrayList<>(ExtendedSyncRecordHandler.EXTENDED_FIELDS);
                            ResultSetMetaData rsmd = rs.getMetaData();
                            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                                String colName = rsmd.getColumnName(i);
                                Optional<DbField> field = extendedFields.stream()
                                        .filter(f -> f.name().equalsIgnoreCase(colName))
                                        .findFirst();
                                field.ifPresent(extendedFields::remove); // field exists in table, so remove it from list
                            }
                            return extendedFields; // this will contain only fields that are missing from the table
                        });

                if (missingFields.size() > 0) {
                    // build a field spec from missing fields (will be empty if no fields)
                    String fieldSpec = missingFields.stream().map(field -> {
                        StringBuilder spec = new StringBuilder();
                        // name and SQL type
                        spec.append("ADD COLUMN ").append(field.name()).append(" ").append(getSqlType(field.type()));
                        // dimension if applicable
                        if (field.dimension() > -1) {
                            spec.append("(").append(field.dimension()).append(")");
                        } else if (ExtendedSyncRecordHandler.ERROR_FIELDS.contains(field)) {
                            spec.append("(").append(maxErrorSize).append(")");
                        }
                        // nullability
                        spec.append(field.nullable() ? "" : " NOT").append(" NULL");
                        return spec.toString();
                    }).collect(Collectors.joining(", "));

                    // update table
                    getJdbcTemplate().update("ALTER TABLE " + getObjectsTableName() + " " + fieldSpec);
                }
            } catch (RuntimeException e) {
                log.error("could not add extended fields to DB table {}. you may have to manually add the columns", getObjectsTableName());
                throw e;
            }
        }
    }

    public String getSqlType(DbField.Type type) {
        if (type == DbField.Type.bool) return "INT";
        else if (type == DbField.Type.string) return "VARCHAR";
        else if (type == DbField.Type.intNumber) return "INT";
        else if (type == DbField.Type.bigIntNumber) return "BIGINT";
        else if (type == DbField.Type.floatNumber) return "FLOAT";
        else if (type == DbField.Type.datetime) return "DATETIME";
        else throw new IllegalArgumentException("unknown DbField.Type " + type);
    }

    @Override
    public Date getResultDate(ResultSet rs, String name) throws SQLException {
        return new Date(rs.getTimestamp(name).getTime());
    }

    @Override
    public Object getDateParam(Date date) {
        if (date == null) return null;
        return new java.sql.Timestamp(date.getTime());
    }
}
