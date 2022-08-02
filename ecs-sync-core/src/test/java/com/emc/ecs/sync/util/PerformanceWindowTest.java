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
package com.emc.ecs.sync.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the PerformanceWindow class
 */
public class PerformanceWindowTest {

    @Test
    public void testPerformanceWindowSum() throws Exception {
        PerformanceWindow pw = new PerformanceWindow(500, 10);

        pw.increment(1000);
        pw.increment(30000);

        // Wait for stats to update
        Thread.sleep(1100);

        assertEquals(31000, pw.getWindowSum(), "Sum incorrect");
        pw.close();
    }

    @Test
    public void testPerformanceWindowDuration() throws Exception {
        PerformanceWindow pw = new PerformanceWindow(500, 10);

        Thread.sleep(6000);

        assertTrue(4500 < pw.getWindowDuration(), "Window should be at least 4500ms");
        pw.close();
    }

    @Test
    public void testPerformanceWindowRate() throws Exception {
        PerformanceWindow pw = new PerformanceWindow(500, 10);

        pw.increment(30000);
        pw.increment(1000);

        Thread.sleep(1100);
        pw.close();

        assertEquals(31000.0, (double)pw.getWindowRate(), 500.0, "Incorrect rate");
    }

}