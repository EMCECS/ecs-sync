package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.filter.ShellCommandConfig;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ShellCommandFilterTest {
    private static final Logger log = LoggerFactory.getLogger(ShellCommandFilterTest.class);

    final String SCRIPT = "" +
            "#!/usr/bin/env bash\n" +
            "if [[ \"$1\" == *fail ]]; then\n" +
            "  >&2 echo \"failed: $1\"\n" +
            "  exit 1\n" +
            "fi\n" +
            "echo \"success: $1\"\n" +
            "exit 0\n";

    final ShellCommandConfig config = new ShellCommandConfig();
    final SyncOptions options = new SyncOptions();
    final TestStorage sourceStorage = new TestStorage();
    final ShellCommandFilter filter = new ShellCommandFilter();
    final VerifyCommandFilter verifyFilter = new VerifyCommandFilter();
    Path scriptFile;

    String[] filePaths = {
            "normal-name",
            "space dir/space file",
            "weirdchars!@#$%^&(¨ÿö╥",
            "r/e/a/l/l/y/d/e/e/p/path",
    };

    SyncObject goodObject1 = new SyncObject(sourceStorage, "normal-name",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
    SyncObject goodObject2 = new SyncObject(sourceStorage, "space dir/space file",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
    SyncObject goodObject3 = new SyncObject(sourceStorage, "weirdchars!@#$%^&(¨ÿö╥",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
    SyncObject goodObject4 = new SyncObject(sourceStorage, "r/e/a/l/l/y/d/e/e/p/path",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());

    SyncObject badObject1 = new SyncObject(sourceStorage, "normal-name-fail",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
    SyncObject badObject2 = new SyncObject(sourceStorage, "space dir/space file fail",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
    SyncObject badObject3 = new SyncObject(sourceStorage, "weirdchars!@#$%^&(¨ÿö╥fail",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());
    SyncObject badObject4 = new SyncObject(sourceStorage, "r/e/a/l/l/y/d/e/e/p/pathfail",
            new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl());

    @Before
    public void before() throws IOException {
        // write script file
        scriptFile = Files.createTempFile("ecs-sync-shell-command-test", "sh");
        Files.write(scriptFile, SCRIPT.getBytes(StandardCharsets.UTF_8));
        log.info("script file: {}", scriptFile);
        // set execute
        Set<PosixFilePermission> perms = Files.getPosixFilePermissions(scriptFile);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        Files.setPosixFilePermissions(scriptFile, perms);

        // config
        config.setShellCommand(scriptFile.toString());

        // filter
        filter.withConfig(config).withOptions(options);
        filter.configure(sourceStorage, Collections.singletonList((SyncFilter) filter).iterator(), null);
        filter.setNext(verifyFilter);
    }

    @After
    public void after() throws IOException {
        if (scriptFile != null) Files.delete(scriptFile);
    }

    @Test
    public void testExecuteBeforeSend() {
        config.setExecuteAfterSending(false);
        config.setFailOnNonZeroExit(true);

        // good objects should all make it to the verify filter
        createGoodObjects().forEach(object -> filter.filter(createContext(object)));
        Assert.assertEquals(Arrays.asList(filePaths), verifyFilter.pathsSeen);

        // bad objects will throw an exception and *not* make it to the verify filter
        createBadObjects().forEach(object -> {
            try {
                filter.filter(createContext(object));
                Assert.fail("filter should have thrown exception");
            } catch (RuntimeException e) {
                Assert.assertTrue("bad message: " + e.getMessage(), e.getMessage().trim().endsWith("failed: test://" + object.getRelativePath()));
            }
        });
        Assert.assertEquals(Arrays.asList(filePaths), verifyFilter.pathsSeen);
    }

    @Test
    public void testExecuteAfterSend() {
        config.setExecuteAfterSending(true);
        config.setFailOnNonZeroExit(true);

        // good objects should all make it to the verify filter
        createGoodObjects().forEach(object -> filter.filter(createContext(object)));
        Assert.assertEquals(Arrays.asList(filePaths), verifyFilter.pathsSeen);

        // bad objects will throw an exception, but *also* make it to the verify filter
        createBadObjects().forEach(object -> {
            try {
                filter.filter(createContext(object));
                Assert.fail("filter should have thrown exception");
            } catch (RuntimeException e) {
                Assert.assertTrue("bad message: " + e.getMessage(), e.getMessage().trim().endsWith("failed: test://" + object.getRelativePath()));
            }
        });
        Stream<String> combinedPaths = Stream.concat(Arrays.stream(filePaths), Arrays.stream(filePaths).map(p -> p + "fail"));
        Assert.assertEquals(combinedPaths.collect(Collectors.toList()), verifyFilter.pathsSeen);
    }

    @Test
    public void testNoFailOnNonZeroExit() {
        config.setExecuteAfterSending(false);
        config.setFailOnNonZeroExit(false);

        // no exceptions should be thrown
        createGoodObjects().forEach(object -> filter.filter(createContext(object)));
        createBadObjects().forEach(object -> filter.filter(createContext(object)));

        // all objects should make it to the verify filter
        Stream<String> combinedPaths = Stream.concat(Arrays.stream(filePaths), Arrays.stream(filePaths).map(p -> p + "fail"));
        Assert.assertEquals(combinedPaths.collect(Collectors.toList()), verifyFilter.pathsSeen);
    }

    List<SyncObject> createGoodObjects() {
        return Arrays.stream(filePaths)
                .map(path -> new SyncObject(sourceStorage, path,
                        new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl()))
                .collect(Collectors.toList());
    }

    List<SyncObject> createBadObjects() {
        return Arrays.stream(filePaths)
                .map(path -> new SyncObject(sourceStorage, path + "fail",
                        new ObjectMetadata(), new ByteArrayInputStream(new byte[0]), new ObjectAcl()))
                .collect(Collectors.toList());
    }

    ObjectContext createContext(SyncObject object) {
        return new ObjectContext()
                .withObject(object)
                .withOptions(options)
                .withStatus(ObjectStatus.InTransfer)
                .withSourceSummary(new ObjectSummary("test://" + object.getRelativePath(), false, 0));
    }

    static class VerifyCommandFilter extends AbstractFilter<AbstractConfig> {
        List<String> pathsSeen = new ArrayList<>();

        @Override
        public void filter(ObjectContext objectContext) {
            pathsSeen.add(objectContext.getObject().getRelativePath());
            objectContext.setTargetId("target://" + objectContext.getObject().getRelativePath());
        }

        @Override
        public SyncObject reverseFilter(ObjectContext objectContext) {
            throw new UnsupportedOperationException();
        }

        public List<String> getPathsSeen() {
            return pathsSeen;
        }
    }
}
