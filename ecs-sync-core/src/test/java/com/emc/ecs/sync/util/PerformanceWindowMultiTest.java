/*
 * Copyright (c) 2015-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by cwikj on 10/30/2015.
 */
public class PerformanceWindowMultiTest {
    private static final Logger log = LoggerFactory.getLogger(PerformanceWindowMultiTest.class);

    @Test
    public void testPerformanceWindow() throws Exception {
        try (PerformanceWindow pw = new PerformanceWindow(500, 10)) {

            ScheduledExecutorService se = Executors.newScheduledThreadPool(5);

            log.warn("Adding one @ 1000b/s");
            long rate = 1000;
            se.scheduleAtFixedRate(() -> pw.increment(1000), 1, 1, TimeUnit.SECONDS);
            Thread.sleep(10000);
            log.warn("expected rate: {}, actual rate: {}", rate, pw.getWindowRate());
            Assertions.assertEquals(rate, pw.getWindowRate(), rate / 10.0); // within 10%

            log.warn("Adding another @ 32kB/s");
            rate += 32 * 1024;
            se.scheduleAtFixedRate(() -> pw.increment(32 * 1024), 1, 1, TimeUnit.SECONDS);
            Thread.sleep(10000);
            log.warn("expected rate: {}, actual rate: {}", rate, pw.getWindowRate());
            Assertions.assertEquals(rate, pw.getWindowRate(), rate / 10.0); // within 10%

            log.warn("Adding a fast one @ 1MB/s");
            rate += 1024 * 1000;
            se.scheduleAtFixedRate(() -> pw.increment(1024), 1, 1, TimeUnit.MILLISECONDS);
            Thread.sleep(10000);
            log.warn("expected rate: {}, actual rate: {}", rate, pw.getWindowRate());
            Assertions.assertEquals(rate, pw.getWindowRate(), rate / 10.0); // within 10%

            log.warn("Adding another fast one @ 100MB/s");
            rate += 128 * 1024 * 1000;
            se.scheduleAtFixedRate(() -> pw.increment(128 * 1024), 1, 1, TimeUnit.MILLISECONDS);
            Thread.sleep(10000);
            log.warn("expected rate: {}, actual rate: {}", rate, pw.getWindowRate());
            Assertions.assertEquals(rate, pw.getWindowRate(), rate / 10.0); // within 10%

            se.shutdownNow();
        }
    }
}
