package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectStatus;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.service.InMemoryDbService;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.storage.TestStorage;
import com.emc.ecs.sync.util.LoggingUtil;
import com.emc.ecs.sync.util.PluginUtil;
import com.emc.ecs.sync.util.VerifyUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public abstract class AbstractEndToEndTest {
    private static final Logger log = LoggerFactory.getLogger(AbstractEndToEndTest.class);

    protected static final int SM_OBJ_COUNT = 200;
    protected static final int SM_OBJ_MAX_SIZE = 10240; // 10K
    protected static final int LG_OBJ_COUNT = 10;
    protected static final int LG_OBJ_MAX_SIZE = 1024 * 1024; // 1M

    protected static final int SYNC_THREAD_COUNT = 32;

    protected ExecutorService service;

    @BeforeEach
    public void before() {
        service = Executors.newFixedThreadPool(SYNC_THREAD_COUNT);
    }

    @AfterEach
    public void after() {
        if (service != null) service.shutdownNow();
    }

    protected void multiEndToEndTest(Object storageConfig, TestConfig testConfig, boolean syncAcl) {
        multiEndToEndTest(storageConfig, testConfig, null, syncAcl);
    }

    protected void multiEndToEndTest(Object storageConfig, TestConfig testConfig, ObjectAcl aclTemplate, boolean syncAcl) {
        if (testConfig == null) testConfig = new TestConfig();
        testConfig.withReadData(true).withDiscardData(false);

        // large objects
        String testName = "large objects";
        testConfig.withObjectCount(LG_OBJ_COUNT).withMaxSize(LG_OBJ_MAX_SIZE);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl, testName, false);

        // small objects
        testName = "small objects";
        testConfig.withObjectCount(SM_OBJ_COUNT).withMaxSize(SM_OBJ_MAX_SIZE);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl, testName, false);

        // zero-byte objects (always important!)
        testName = "zero-byte objects";
        testConfig.withObjectCount(SM_OBJ_COUNT).withMaxSize(0);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl, testName, false);

        // use extended DB fields
        testName = "extended DB fields";
        testConfig.withObjectCount(SM_OBJ_COUNT).withMaxSize(SM_OBJ_MAX_SIZE);
        endToEndTest(storageConfig, testConfig, aclTemplate, syncAcl, testName, true);
    }

    protected void endToEndTest(Object storageConfig, TestConfig testConfig, ObjectAcl aclTemplate, boolean syncAcl, String testName, boolean extendedDbFields) {
        // TODO: handle log elevation in a different way
        LoggingUtil.setRootLogLevel(Level.INFO);
        SyncOptions options = new SyncOptions().withThreadCount(SYNC_THREAD_COUNT);
        options.withSyncAcl(syncAcl).withTimingsEnabled(true).withTimingWindow(100).withDbEnhancedDetailsEnabled(extendedDbFields);
        options.setRememberFailed(true);

        // set up DB table
        String tableName = "t" + System.currentTimeMillis();
        log.info("generated DB table name is {}", tableName);
        InMemoryDbService dbService = new InMemoryDbService(options.isDbEnhancedDetailsEnabled());
        dbService.setObjectsTableName(tableName);

        // make sure the database is clean
        dbService.initCheck();
        Assertions.assertEquals(0, dbService.getJdbcTemplate().queryForObject("select count(*) from " + tableName, Long.class).longValue(),
                "in-memory database table is not clean");

        // create test source
        TestStorage testSource = new TestStorage();
        testSource.withAclTemplate(aclTemplate).withConfig(testConfig).withOptions(options);

        try {
            // send test data to test system
            String jobName = testName + " - sync+verify to target";
            options.setVerify(true);
            EcsSync sync = new EcsSync();
            sync.setSource(testSource); // must use the same source for consistency
            sync.setSyncConfig(new SyncConfig().withTarget(storageConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.run();
            options.setVerify(false); // revert options

            String summary = summarizeFailure(jobName, sync);
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed(), summary);

            // test verify-only in target
            jobName = testName + " - verify-only in target";
            options.setVerifyOnly(true);
            sync = new EcsSync();
            sync.setSource(testSource); // must use the same source for consistency
            sync.setSyncConfig(new SyncConfig().withTarget(storageConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.run();
            options.setVerifyOnly(false); // revert options

            summary = summarizeFailure(jobName, sync);
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed(), summary);

            // read data from same system
            jobName = testName + " - read+verify from source";
            options.setVerify(true);
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withTarget(testConfig).withOptions(options));
            sync.setPerfReportSeconds(2);
            sync.setDbService(dbService);
            sync.run();
            options.setVerify(false); // revert options

            // save test target for verify-only
            TestStorage testTarget = (TestStorage) sync.getTarget();

            summary = summarizeFailure(jobName, sync);
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed(), summary);
            verifyDb(testSource, dbService, extendedDbFields);
            Assertions.assertEquals(sync.getStats().getObjectsComplete(), sync.getEstimatedTotalObjects(), summary);
            Assertions.assertEquals(sync.getStats().getBytesComplete(), sync.getEstimatedTotalBytes(), summary);

            // test verify-only in source
            jobName = testName + " - verify-only in source";
            options.setVerifyOnly(true);
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withOptions(options));
            sync.setTarget(testTarget);
            sync.setPerfReportSeconds(2);
            sync.setDbService(dbService);
            sync.run();
            options.setVerifyOnly(false); // revert options

            summary = summarizeFailure(jobName, sync);
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed(), summary);
            verifyDb(testSource, dbService, extendedDbFields);

            VerifyUtil.verifyObjects(testSource, testSource.getRootObjects(), testTarget, testTarget.getRootObjects(), syncAcl);

            // test source list operation
            // TODO: implement

            // test source list file operation
            jobName = testName + " - read+verify+list-file from source";
            File listFile = createListFile(sync.getSource()); // should be the real storage plugin (not test)
            options.setSourceListFile(listFile.getPath());
            sync = new EcsSync();
            sync.setSyncConfig(new SyncConfig().withSource(storageConfig).withTarget(testConfig).withOptions(options));
            sync.run();
            options.setSourceListFile(null); // revert options

            summary = summarizeFailure(jobName, sync);
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed(), summary);
            Assertions.assertEquals(0, sync.getStats().getObjectsFailed(), summary);

            testTarget = (TestStorage) sync.getTarget();

            VerifyUtil.verifyObjects(testSource, testSource.getRootObjects(), testTarget, testTarget.getRootObjects(), syncAcl);
        } finally {
            try {
                // delete the objects from the test system
                SyncStorage<?> storage = PluginUtil.newStorageFromConfig(storageConfig, options);
                storage.configure(storage, Collections.emptyIterator(), null);
                List<Future<?>> futures = new ArrayList<>();
                for (ObjectSummary summary : storage.allObjects()) {
                    futures.add(recursiveDelete(storage, summary));
                }
                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (Throwable t) {
                        log.warn("error deleting object", t);
                    }
                }
            } catch (Throwable t) {
                log.warn("could not delete objects after sync: " + t.getMessage());
            }
            try {
                // destroy the database to make sure it isn't somehow reused (not sure how this happens)
                dbService.deleteDatabase();
                dbService.close();
            } catch (Exception e) {
                log.warn("could not close dbService", e);
            }
        }
    }

    protected String summarizeFailure(String jobName, EcsSync sync) {
        String summary = "job " + jobName + " failed:\n";
        for (String failedObject : sync.getStats().getFailedObjects()) {
            summary += failedObject + "\n";
        }
        return summary;
    }

    protected File createListFile(SyncStorage<?> storage) {
        try {
            File listFile = File.createTempFile("list-file", null);
            listFile.deleteOnExit();

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(listFile))) {
                listIdentifiers(writer, storage, storage.allObjects());
                writer.flush();
            }

            return listFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void listIdentifiers(BufferedWriter writer, SyncStorage<?> storage, Iterable<ObjectSummary> summaries) throws IOException {
        for (ObjectSummary summary : summaries) {
            writer.append(summary.getIdentifier()).append("\n");
            if (summary.isDirectory()) listIdentifiers(writer, storage, storage.children(summary));
        }
    }

    protected Future<?> recursiveDelete(final SyncStorage<?> storage, final ObjectSummary object) {
        final List<Future<?>> futures = new ArrayList<>();
        if (object.isDirectory()) {
            for (ObjectSummary child : storage.children(object)) {
                futures.add(recursiveDelete(storage, child));
            }
        }
        return service.submit(() -> {
            for (Future<?> future : futures) {
                future.get();
            }
            try {
                log.info("deleting {}", object.getIdentifier());
                storage.delete(object.getIdentifier(), null);
            } catch (Throwable t) {
                log.warn("could not delete " + object.getIdentifier(), t);
            }
            return null;
        });
    }

    protected void verifyDb(TestStorage storage, InMemoryDbService dbService, boolean extendedFields) {
        log.info("verifying test storage against database {}", dbService.getObjectsTableName());

        long totalCount = verifyDbObjects(dbService, storage, storage.getRootObjects(), extendedFields);

        SqlRowSet rowSet = dbService.getJdbcTemplate().queryForRowSet("SELECT count(target_id) FROM " + dbService.getObjectsTableName() + " WHERE target_id != ''");
        Assertions.assertTrue(rowSet.next());
        Assertions.assertEquals(totalCount, rowSet.getLong(1));
    }

    protected long verifyDbObjects(InMemoryDbService dbService, TestStorage storage,
                                   Collection<? extends SyncObject> objects, boolean extendedFields) {
        Date now = new Date();
        long count = 0;
        for (SyncObject object : objects) {
            count++;
            String identifier = storage.getIdentifier(object.getRelativePath(), object.getMetadata().isDirectory());
            SqlRowSet rowSet = dbService.getJdbcTemplate().queryForRowSet("SELECT * FROM " + dbService.getObjectsTableName() + " WHERE target_id=?",
                    identifier);
            Assertions.assertTrue(rowSet.next());
            Assertions.assertEquals(identifier, rowSet.getString("target_id"));
            Assertions.assertEquals(object.getMetadata().isDirectory(), rowSet.getBoolean("is_directory"));
            Assertions.assertEquals(object.getMetadata().getContentLength(), rowSet.getLong("size"));
            // mtime in the DB is actually pulled from the target system, so we don't know what precision it will be in
            // or if the target system's clock is in sync, but let's assume it will always be within 5 minutes
            Assertions.assertTrue(Math.abs(object.getMetadata().getModificationTime().getTime() - rowSet.getLong("mtime")) < 5 * 60 * 1000);
            Assertions.assertEquals(ObjectStatus.Verified.getValue(), rowSet.getString("status"));
            long transferStart = rowSet.getLong("transfer_start"), transferComplete = rowSet.getLong("transfer_complete");
            if (transferStart > 0)
                Assertions.assertTrue(now.getTime() - transferStart < 10 * 60 * 1000); // less than 10 minutes ago
            if (transferComplete > 0)
                Assertions.assertTrue(now.getTime() - transferComplete < 10 * 60 * 1000); // less than 10 minutes ago
            Assertions.assertTrue(now.getTime() - rowSet.getLong("verify_start") < 10 * 60 * 1000); // less than 10 minutes ago
            Assertions.assertTrue(now.getTime() - rowSet.getLong("verify_complete") < 10 * 60 * 1000); // less than 10 minutes ago
            Assertions.assertEquals(0, rowSet.getInt("retry_count"));
            if (extendedFields) {
                if (!object.getMetadata().isDirectory()) {
                    Assertions.assertEquals(object.getMd5Hex(true), rowSet.getString("source_md5"));
                    Assertions.assertNull(rowSet.getString("source_retention_end_time"));
                    Assertions.assertEquals(object.getMd5Hex(true), rowSet.getString("target_md5"));
                    Assertions.assertNull(rowSet.getString("target_retention_end_time"));
                }
                Assertions.assertTrue(rowSet.getLong("target_mtime") >= rowSet.getLong("mtime"));
                Assertions.assertNull(rowSet.getString("first_error_message"));
            }
            if (object.getMetadata().isDirectory())
                count += verifyDbObjects(dbService, storage, storage.getChildren(identifier), extendedFields);
        }
        return count;
    }
}
