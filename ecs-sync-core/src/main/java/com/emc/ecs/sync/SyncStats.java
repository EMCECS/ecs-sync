/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync;

import com.emc.ecs.sync.model.FailedObject;
import com.emc.ecs.sync.util.PerformanceWindow;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class SyncStats implements AutoCloseable {
    // objectsComplete + objectsSkipped + objectsFailed = (total objects)
    // Counted if any phase of the sync has been completed and the object sync is successful.
    private long objectsComplete;
    // Counted if all phases of the sync have been skipped.
    private long objectsSkipped;
    // The number of Objects encountered failure during Sync(source object deletion failure is not counted).
    private long objectsFailed;
    private long bytesComplete, bytesSkipped, pastRunTime, startTime, stopTime, cpuStartTime;
    // Specifically track whether the copy phase is skipped or not.
    private long objectsCopySkipped;
    private long bytesCopySkipped;
    private SortedSet<FailedObject> failedObjects = Collections.synchronizedSortedSet(new TreeSet<>());
    private final PerformanceWindow objectCompleteRate = new PerformanceWindow(500, 20);
    private final PerformanceWindow objectSkipRate = new PerformanceWindow(500, 20);
    private final PerformanceWindow objectErrorRate = new PerformanceWindow(500, 20);

    @Override
    public void close() {
        objectCompleteRate.close();
        objectSkipRate.close();
        objectErrorRate.close();
    }

    public synchronized void reset() {
        objectsComplete = objectsSkipped = objectsFailed = objectsCopySkipped = 0;
        bytesComplete = bytesSkipped = bytesCopySkipped = 0;
        failedObjects = Collections.synchronizedSortedSet(new TreeSet<>());
    }

    public synchronized void incObjectsComplete() {
        objectsComplete++;
        objectCompleteRate.increment(1);
    }

    public synchronized void incObjectsSkipped() {
        objectsSkipped++;
        objectSkipRate.increment(1);
    }

    public synchronized void incObjectsFailed() {
        objectsFailed++;
        objectErrorRate.increment(1);
    }

    public synchronized void incObjectsCopySkipped() {
        objectsCopySkipped++;
    }

    public synchronized void incBytesComplete(long bytes) {
        bytesComplete += bytes;
    }

    public synchronized void incBytesSkipped(long bytes) {
        bytesSkipped += bytes;
    }

    public synchronized void incBytesCopySkipped(long bytes) {
        bytesCopySkipped += bytes;
    }

    public long getObjectCompleteRate() {
        return objectCompleteRate.getWindowRate();
    }

    public long getObjectSkipRate() {
        return objectSkipRate.getWindowRate();
    }

    public long getObjectErrorRate() {
        return objectErrorRate.getWindowRate();
    }

    public synchronized void pause() {
        stopTime = System.currentTimeMillis();
    }

    public synchronized void resume() {
        pastRunTime += stopTime - startTime;
        startTime = System.currentTimeMillis();
        stopTime = 0;
    }

    public void addFailedObject(String name) {
        addFailedObject(new FailedObject(name));
    }

    public void addFailedObject(FailedObject failedObject) {
        failedObjects.add(failedObject);
    }

    public long getTotalRunTime() {
        if (startTime == 0) return 0;
        long last = stopTime > 0 ? stopTime : System.currentTimeMillis();
        return pastRunTime + (last - startTime);
    }

    /**
     * Returns the CPU time consumed by a sync process in milliseconds
     */
    public long getTotalCpuTime() {
        if (cpuStartTime == 0) return 0;
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() / 1000000 - cpuStartTime;

    }

    public String getStatsString() {
        long secs = (System.currentTimeMillis() - startTime) / 1000L;
        if (secs == 0) secs = 1;
        long byteRate = bytesComplete / secs;
        double objectRate = (double) objectsComplete / secs;

        return MessageFormat.format("Transferred {0} bytes in {1} seconds ({2} bytes/s) - skipped {3} bytes\n",
                bytesComplete, secs, byteRate, bytesSkipped) +
                MessageFormat.format("Successful files: {0} ({2,number,#.##}/s) Skipped files: {3} Failed Files: {1}\n",
                        objectsComplete, objectsFailed, objectRate, objectsSkipped) +
                MessageFormat.format("Failed files: {0}\n", failedObjects);
    }

    public long getObjectsComplete() {
        return objectsComplete;
    }

    public long getObjectsSkipped() {
        return objectsSkipped;
    }

    public long getObjectsFailed() {
        return objectsFailed;
    }

    public long getObjectsCopySkipped() {
        return objectsCopySkipped;
    }

    public long getBytesComplete() {
        return bytesComplete;
    }

    public long getBytesSkipped() {
        return bytesSkipped;
    }

    public long getBytesCopySkipped() {
        return bytesCopySkipped;
    }

    public long getPastRunTime() {
        return pastRunTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public void setStopTime(long stopTime) {
        this.stopTime = stopTime;
    }

    public long getCpuStartTime() {
        return cpuStartTime;
    }

    public void setCpuStartTime(long cpuStartTime) {
        this.cpuStartTime = cpuStartTime;
    }

    public Set<String> getFailedObjects() {
        return failedObjects.stream().map(FailedObject::getIdentifier).collect(Collectors.toSet());
    }

    public SortedSet<FailedObject> getFailedObjectDetails() {
        return failedObjects;
    }
}
