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
package com.emc.ecs.sync.rest;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

@XmlRootElement
public class SyncProgress {
    private String jobName;
    private JobControlStatus status;
    private long syncStartTime;
    private long syncStopTime;
    private boolean estimatingTotals;
    private long totalBytesExpected;
    private long totalObjectsExpected;
    private long bytesComplete;
    private long bytesSkipped;
    private long objectsComplete;
    private long objectsSkipped;
    private long objectsFailed;
    private int objectsAwaitingRetry;
    private long runtimeMs;
    private int activeQueryTasks;
    private int activeSyncTasks;
    private long cpuTimeMs;
    private double processCpuLoad;
    private long processMemoryUsed;
    private long objectCompleteRate;
    private long objectSkipRate;
    private long objectErrorRate;
    private long sourceReadRate;
    private long sourceWriteRate;
    private long targetReadRate;
    private long targetWriteRate;
    private String runError;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public JobControlStatus getStatus() {
        return status;
    }

    public void setStatus(JobControlStatus status) {
        this.status = status;
    }

    public long getSyncStartTime() {
        return syncStartTime;
    }

    public void setSyncStartTime(long syncStartTime) {
        this.syncStartTime = syncStartTime;
    }

    public long getSyncStopTime() {
        return syncStopTime;
    }

    public void setSyncStopTime(long syncStopTime) {
        this.syncStopTime = syncStopTime;
    }

    public boolean isEstimatingTotals() {
        return estimatingTotals;
    }

    public void setEstimatingTotals(boolean estimatingTotals) {
        this.estimatingTotals = estimatingTotals;
    }

    public long getTotalBytesExpected() {
        return totalBytesExpected;
    }

    public void setTotalBytesExpected(long totalBytesExpected) {
        this.totalBytesExpected = totalBytesExpected;
    }

    public long getTotalObjectsExpected() {
        return totalObjectsExpected;
    }

    public void setTotalObjectsExpected(long totalObjectsExpected) {
        this.totalObjectsExpected = totalObjectsExpected;
    }

    public long getBytesComplete() {
        return bytesComplete;
    }

    public void setBytesComplete(long bytesComplete) {
        this.bytesComplete = bytesComplete;
    }

    public long getBytesSkipped() {
        return bytesSkipped;
    }

    public void setBytesSkipped(long bytesSkipped) {
        this.bytesSkipped = bytesSkipped;
    }

    public long getObjectsComplete() {
        return objectsComplete;
    }

    public void setObjectsComplete(long objectsComplete) {
        this.objectsComplete = objectsComplete;
    }

    public long getObjectsSkipped() {
        return objectsSkipped;
    }

    public void setObjectsSkipped(long objectsSkipped) {
        this.objectsSkipped = objectsSkipped;
    }

    public long getObjectsFailed() {
        return objectsFailed;
    }

    public void setObjectsFailed(long objectsFailed) {
        this.objectsFailed = objectsFailed;
    }

    public int getObjectsAwaitingRetry() {
        return objectsAwaitingRetry;
    }

    public void setObjectsAwaitingRetry(int objectsAwaitingRetry) {
        this.objectsAwaitingRetry = objectsAwaitingRetry;
    }

    public long getRuntimeMs() {
        return runtimeMs;
    }

    public void setRuntimeMs(long runtimeMs) {
        this.runtimeMs = runtimeMs;
    }

    public int getActiveQueryTasks() {
        return activeQueryTasks;
    }

    public void setActiveQueryTasks(int activeQueryTasks) {
        this.activeQueryTasks = activeQueryTasks;
    }

    public int getActiveSyncTasks() {
        return activeSyncTasks;
    }

    public void setActiveSyncTasks(int activeSyncTasks) {
        this.activeSyncTasks = activeSyncTasks;
    }

    public long getCpuTimeMs() {
        return cpuTimeMs;
    }

    public void setCpuTimeMs(long cpuTimeMs) {
        this.cpuTimeMs = cpuTimeMs;
    }

    public double getProcessCpuLoad() {
        return processCpuLoad;
    }

    public void setProcessCpuLoad(double processCpuLoad) {
        this.processCpuLoad = processCpuLoad;
    }

    public long getProcessMemoryUsed() {
        return processMemoryUsed;
    }

    public void setProcessMemoryUsed(long processMemoryUsed) {
        this.processMemoryUsed = processMemoryUsed;
    }

    public long getSourceReadRate() {
        return sourceReadRate;
    }

    public void setSourceReadRate(long sourceReadRate) {
        this.sourceReadRate = sourceReadRate;
    }

    public long getSourceWriteRate() {
        return sourceWriteRate;
    }

    public void setSourceWriteRate(long sourceWriteRate) {
        this.sourceWriteRate = sourceWriteRate;
    }

    public long getTargetReadRate() {
        return targetReadRate;
    }

    public void setTargetReadRate(long targetReadRate) {
        this.targetReadRate = targetReadRate;
    }

    public long getTargetWriteRate() {
        return targetWriteRate;
    }

    public void setTargetWriteRate(long targetWriteRate) {
        this.targetWriteRate = targetWriteRate;
    }

    public long getObjectCompleteRate() {
        return objectCompleteRate;
    }

    public void setObjectCompleteRate(long objectCompleteRate) {
        this.objectCompleteRate = objectCompleteRate;
    }

    public long getObjectSkipRate() {
        return objectSkipRate;
    }

    public void setObjectSkipRate(long objectSkipRate) {
        this.objectSkipRate = objectSkipRate;
    }

    public long getObjectErrorRate() {
        return objectErrorRate;
    }

    public void setObjectErrorRate(long objectErrorRate) {
        this.objectErrorRate = objectErrorRate;
    }

    public String getRunError() {
        return runError;
    }

    public void setRunError(String runError) {
        this.runError = runError;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SyncProgress that = (SyncProgress) o;
        return syncStartTime == that.syncStartTime
                && syncStopTime == that.syncStopTime
                && estimatingTotals == that.estimatingTotals
                && totalBytesExpected == that.totalBytesExpected
                && totalObjectsExpected == that.totalObjectsExpected
                && bytesComplete == that.bytesComplete
                && bytesSkipped == that.bytesSkipped
                && objectsComplete == that.objectsComplete
                && objectsSkipped == that.objectsSkipped
                && objectsFailed == that.objectsFailed
                && objectsAwaitingRetry == that.objectsAwaitingRetry
                && runtimeMs == that.runtimeMs
                && activeQueryTasks == that.activeQueryTasks
                && activeSyncTasks == that.activeSyncTasks
                && cpuTimeMs == that.cpuTimeMs
                && Double.compare(that.processCpuLoad, processCpuLoad) == 0
                && processMemoryUsed == that.processMemoryUsed
                && objectCompleteRate == that.objectCompleteRate
                && objectSkipRate == that.objectSkipRate
                && objectErrorRate == that.objectErrorRate
                && sourceReadRate == that.sourceReadRate
                && sourceWriteRate == that.sourceWriteRate
                && targetReadRate == that.targetReadRate
                && targetWriteRate == that.targetWriteRate
                && Objects.equals(jobName, that.jobName)
                && status == that.status
                && Objects.equals(runError, that.runError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobName, status,
                syncStartTime, syncStopTime,
                estimatingTotals, totalBytesExpected,
                totalObjectsExpected, bytesComplete,
                bytesSkipped, objectsComplete,
                objectsSkipped, objectsFailed,
                objectsAwaitingRetry, runtimeMs,
                activeQueryTasks, activeSyncTasks,
                cpuTimeMs, processCpuLoad,
                processMemoryUsed, objectCompleteRate,
                objectSkipRate, objectErrorRate,
                sourceReadRate, sourceWriteRate,
                targetReadRate, targetWriteRate,
                runError);
    }
}
