package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.LocalCacheConfig;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class LocalCacheFilterTest {
    private static final Logger log = LoggerFactory.getLogger(LocalCacheFilterTest.class);

    private File cacheDir;
    private final SyncOptions syncOptions = new SyncOptions();

    @BeforeEach
    public void setup() throws Exception {
        cacheDir = Files.createTempDirectory("ecs-sync-cache-filter-test").toFile();
        if (!cacheDir.exists() || !cacheDir.isDirectory()) throw new RuntimeException("unable to make cache dir");
        log.info("cache directory: {}", cacheDir);
    }

    @AfterEach
    public void teardown() throws Exception {
        for (File file : cacheDir.listFiles()) {
            file.delete();
        }
        cacheDir.delete();
    }

    @Test
    public void testLocalCache() {
        LocalCacheConfig config = new LocalCacheConfig();
        config.setLocalCacheRoot(cacheDir.getAbsolutePath());

        // doesn't really matter what this is; it just can't be null
        TestStorage sourceStorage = new TestStorage();

        LocalCacheFilter filter = new LocalCacheFilter();
        filter.setConfig(config);
        filter.setOptions(syncOptions);
        filter.configure(sourceStorage, Collections.singletonList((SyncFilter<?>) filter).iterator(), null);
        filter.setNext(new VerifyCacheFilter());

        SyncObject object1 = new SyncObject(sourceStorage, "normal-name",
                new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
        SyncObject object2 = new SyncObject(sourceStorage, "space dir/space file",
                new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
        SyncObject object3 = new SyncObject(sourceStorage, "weirdchars!@#$%^&(¨ÿö╥",
                new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
        SyncObject object4 = new SyncObject(sourceStorage, "r/e/a/l/l/y/d/e/e/p/path",
                new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());

        // verify cache file is written
        filter.filter(createContext(object1));
        filter.filter(createContext(object2));
        filter.filter(createContext(object3));
        filter.filter(createContext(object4));

        // verify cache files are cleaned up
        Assertions.assertFalse(Files.exists(Paths.get(cacheDir.getAbsolutePath(), object1.getRelativePath())));
        Assertions.assertFalse(Files.exists(Paths.get(cacheDir.getAbsolutePath(), object2.getRelativePath())));
        Assertions.assertFalse(Files.exists(Paths.get(cacheDir.getAbsolutePath(), object3.getRelativePath())));
        Assertions.assertFalse(Files.exists(Paths.get(cacheDir.getAbsolutePath(), object4.getRelativePath())));
    }

    ObjectContext createContext(SyncObject object) {
        return new ObjectContext()
                .withObject(object)
                .withOptions(syncOptions)
                .withStatus(ObjectStatus.InTransfer)
                .withSourceSummary(new ObjectSummary("test://" + object.getRelativePath(), false, 0));
    }

    class VerifyCacheFilter extends AbstractFilter<AbstractConfig> {
        @Override
        public void filter(ObjectContext objectContext) {
            Path cachePath = Paths.get(cacheDir.getAbsolutePath(), objectContext.getObject().getRelativePath());
            Assertions.assertTrue(Files.exists(cachePath));
            Assertions.assertTrue(Files.isRegularFile(cachePath));
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            throw new UnsupportedOperationException();
        }
    }
}
