package com.emc.vipr.sync.util;

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
