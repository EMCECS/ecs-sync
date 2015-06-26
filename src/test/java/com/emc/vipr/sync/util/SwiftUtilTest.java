package com.emc.vipr.sync.util;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by cwikj on 6/26/15.
 */
public class SwiftUtilTest {

    @Test
    public void testParseUri() throws Exception {
        SwiftUtil.SwiftUri uri = SwiftUtil.parseUri("swift:http://username:password@endpoint/root");
        Assert.assertEquals("username", uri.username);
        Assert.assertEquals("password", uri.password);
        Assert.assertEquals("http://endpoint", uri.endpoint);
        Assert.assertEquals("root", uri.rootKey);
    }
}