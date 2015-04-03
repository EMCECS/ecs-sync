package com.emc.vipr.sync.test.util;

import com.emc.vipr.sync.util.CountingInputStream;
import com.emc.vipr.sync.util.SyncUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;

public class CountingInputStreamTest {
    @Test
    public void testCIS() throws Exception {
        int dataSize = 1038;
        byte[] buffer = new byte[dataSize];
        CountingInputStream cis = new CountingInputStream(new ByteArrayInputStream(buffer));
        cis.read();
        cis.read();
        SyncUtil.consumeStream(cis);
        Assert.assertEquals(dataSize, cis.getBytesRead());
    }
}
