package com.emc.ecs.sync.test;

import com.emc.ecs.sync.TargetFilter;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Random;

public class ByteAlteringFilterTest {
    @Test
    public void testByteAlteringFilter() {
        Random random = new Random();
        byte[] buffer = new byte[1024];
        int totalCount = 10000, errorCount = 0;

        ByteAlteringFilter filter = new ByteAlteringFilter();
        filter.setConfig(new ByteAlteringFilter.ByteAlteringConfig());
        TestStorage target = new TestStorage();
        target.setConfig(new TestConfig().withReadData(true).withDiscardData(false));
        filter.setNext(new TargetFilter(target, new SyncOptions()));

        for (int i = 0; i < totalCount; i++) {
            random.nextBytes(buffer);
            String id = "foo" + i;
            SyncObject object = new SyncObject(target, id, new ObjectMetadata().withContentLength(buffer.length),
                    new ByteArrayInputStream(buffer), null);
            target.createObject(object);
            String originalMd5 = object.getMd5Hex(true);
            SyncObject newObject = filter.reverseFilter(new ObjectContext().withSourceSummary(
                    new ObjectSummary(id, false, buffer.length)).withObject(object));
            String newMd5 = newObject.getMd5Hex(true);
            if (!originalMd5.equals(newMd5)) {
                errorCount++;
            }
        }

        Assertions.assertTrue(errorCount > 0);
        Assertions.assertNotEquals(errorCount, totalCount);
        Assertions.assertEquals(errorCount, filter.getConfig().getModifiedObjects());
    }
}
