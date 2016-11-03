package com.emc.ecs.sync;

import com.emc.ecs.sync.util.PerformanceWindow;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

public class SyncStats implements AutoCloseable {
    private long objectsComplete, objectsFailed;
    private long bytesComplete, pastRunTime, startTime, stopTime, cpuStartTime;
    private Set<String> failedObjects = new HashSet<>();
    private PerformanceWindow objectCompleteRate = new PerformanceWindow(500, 20);
    private PerformanceWindow objectErrorRate = new PerformanceWindow(500, 20);

    @Override
    public void close() {
        objectCompleteRate.close();
        objectErrorRate.close();
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize(); // make sure we call super.finalize() no matter what!
        }
    }

    public void reset() {
        objectsComplete = objectsFailed = 0;
        bytesComplete = 0;
        failedObjects = new HashSet<>();
    }

    public synchronized void incObjectsComplete() {
        objectsComplete++;
        objectCompleteRate.increment(1);
    }

    public synchronized void incObjectsFailed() {
        objectsFailed++;
        objectErrorRate.increment(1);
    }

    public synchronized void incBytesComplete(long bytes) {
        bytesComplete += bytes;
    }

    public long getObjectCompleteRate() {
        return objectCompleteRate.getWindowRate();
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
        failedObjects.add(name);
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

        return MessageFormat.format("Transferred {0} bytes in {1} seconds ({2} bytes/s)\n", bytesComplete, secs, byteRate) +
                MessageFormat.format("Successful files: {0} ({2,number,#.##}/s) Failed Files: {1}\n",
                        objectsComplete, objectsFailed, objectRate) +
                MessageFormat.format("Failed files: {0}\n", failedObjects);
    }

    public long getObjectsComplete() {
        return objectsComplete;
    }

    public long getObjectsFailed() {
        return objectsFailed;
    }

    public long getBytesComplete() {
        return bytesComplete;
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
        return failedObjects;
    }
}
