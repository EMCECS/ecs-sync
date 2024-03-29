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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks statistics for a measurement using a sliding window.  For example, this class can track bytes transferred
 * over time and provide an average bytes/second over the window.
 * <p>
 * Note: Each instance of this class creates a background thread, so it's vital to close every instance to shut down
 * these threads. This class will log a warning if >500 instances are created, which probably indicates a
 * resource leak.
 */
public class PerformanceWindow implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PerformanceWindow.class);
    private static final AtomicInteger instanceCount = new AtomicInteger();
    private static final int INSTANCE_COUNT_WARNING_LIMIT = 500;
    private final long sliceInterval;
    private final int sliceCount;

    private final AtomicLong currentValue;
    private final ScheduledExecutorService updater;
    private final LinkedList<PerformanceSlice> slices;

    private long windowSum;
    private long windowDuration;
    private long windowRate;
    private long currentWindowStart;

    /**
     * Creates a new performance window
     * @param sliceInterval size of a slice of the window in milliseconds
     * @param sliceCount number of slices in the window.
     */
    public PerformanceWindow(long sliceInterval, int sliceCount) {
        this.sliceInterval = sliceInterval;
        this.sliceCount = sliceCount;

        currentValue = new AtomicLong();
        currentWindowStart = System.currentTimeMillis();
        slices = new LinkedList<>();
        updater = Executors.newSingleThreadScheduledExecutor();
        updater.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                update();
            }
        }, sliceInterval, sliceInterval, TimeUnit.MILLISECONDS);

        int currentInstanceCount = instanceCount.incrementAndGet();
        if (currentInstanceCount > INSTANCE_COUNT_WARNING_LIMIT) {
            log.warn("{} PerformanceWindow instances detected - there may be a resource leak!", currentInstanceCount);
        } else {
            log.debug("new PerformanceWindow instance created - {} total instances active", currentInstanceCount);
        }
    }

    /**
     * Increments the current slice in the performance window.
     * @param value the number of items (e.g. bytes) to increment the counter by.
     */
    public void increment(final long value) {
        currentValue.addAndGet(value);
    }

    /**
     * Called by the timer.  Updates the statistics.
     */
    private void update() {
        // Push the current slice into the list and update stats.
        long now = System.currentTimeMillis();
        long value = currentValue.getAndSet(0);
        PerformanceSlice completedSlice = new PerformanceSlice();
        completedSlice.sliceStart = currentWindowStart;
        completedSlice.sliceEnd = now;
        currentWindowStart = now;
        completedSlice.value = value;
        slices.add(completedSlice);

        log.trace("New sample: start: {} end: {} value: {}", completedSlice.sliceStart,
                completedSlice.sliceEnd, completedSlice.value);

        long sum = 0;
        long startTime = Long.MAX_VALUE;
        long endTime = 0;
        long maxAge = sliceInterval*(sliceCount) + 50; // 50 ms fudge for timing.
        for(Iterator<PerformanceSlice> i = slices.iterator(); i.hasNext();) {
            PerformanceSlice p = i.next();
            if(now - p.sliceStart > maxAge) {
                i.remove();
                continue;
            }
            sum += p.value;
            startTime = Math.min(startTime, p.sliceStart);
            endTime = Math.max(endTime, p.sliceEnd);
        }
        long duration = endTime - startTime;
        long rate = (long)((double)sum / (duration/1000.0));
        //long rate = sum / (duration/1000L);

        synchronized(this) {
            this.windowSum = sum;
            this.windowDuration = duration;
            this.windowRate = rate;
            log.trace("Stat update: sum={} duration={} rate={}", sum, duration, rate);
        }
    }

    @Override
    public void close() {
        try {
            updater.shutdownNow();
        } catch (Throwable t) {
            log.warn("could not shut down updater", t);
        } finally {
            instanceCount.decrementAndGet();
        }
    }

    /**
     * Gets the current sum for the performance window.
     * @return sum of the slice counters in the window
     */
    public long getWindowSum() {
        return windowSum;
    }

    /**
     * Gets the current duration of the performance window in milliseconds.  Usually this will be sliceInterval *
     * sliceCount but during startup it may be shorter.
     * @return current window duration in milliseconds
     */
    public long getWindowDuration() {
        return windowDuration;
    }

    /**
     * Gets the rate in items/s for the current window.
     * @return the rate in items/s.
     */
    public long getWindowRate() {
        return windowRate;
    }

    /**
     * Inner class used to track the time slices in the current window.
     */
    class PerformanceSlice {
        long sliceStart;
        long sliceEnd;
        long value;
    }
}
