/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.emc.ecs.sync.bean;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "SyncProgress")
@XmlType(propOrder = {"syncStartTime", "syncStopTime", "estimatingTotals", "totalBytesExpected", "totalObjectsExpected",
        "bytesComplete", "objectsComplete", "objectsFailed", "runtimeMs", "activeQueryTasks", "activeSyncTasks",
        "cpuTimeMs", "processCpuLoad", "processMemoryUsed", "objectCompleteRate", "objectErrorRate", "sourceReadRate",
        "sourceWriteRate", "targetReadRate", "targetWriteRate", "runError"})
public class SyncProgress {
    private long syncStartTime;
    private long syncStopTime;
    private boolean estimatingTotals;
    private long totalBytesExpected;
    private long totalObjectsExpected;
    private long bytesComplete;
    private long objectsComplete;
    private long objectsFailed;
    private long runtimeMs;
    private int activeQueryTasks;
    private int activeSyncTasks;
    private long cpuTimeMs;
    private double processCpuLoad;
    private long processMemoryUsed;
    private long objectCompleteRate;
    private long objectErrorRate;
    private long sourceReadRate;
    private long sourceWriteRate;
    private long targetReadRate;
    private long targetWriteRate;
    private String runError;

    @XmlElement(name = "SyncStartTime")
    public long getSyncStartTime() {
        return syncStartTime;
    }

    public void setSyncStartTime(long syncStartTime) {
        this.syncStartTime = syncStartTime;
    }

    @XmlElement(name = "SyncStopTime")
    public long getSyncStopTime() {
        return syncStopTime;
    }

    public void setSyncStopTime(long syncStopTime) {
        this.syncStopTime = syncStopTime;
    }

    @XmlElement(name = "EstimatingTotals")
    public boolean isEstimatingTotals() {
        return estimatingTotals;
    }

    public void setEstimatingTotals(boolean estimatingTotals) {
        this.estimatingTotals = estimatingTotals;
    }

    @XmlElement(name = "TotalBytesExpected")
    public long getTotalBytesExpected() {
        return totalBytesExpected;
    }

    public void setTotalBytesExpected(long totalBytesExpected) {
        this.totalBytesExpected = totalBytesExpected;
    }

    @XmlElement(name = "TotalObjectsExpected")
    public long getTotalObjectsExpected() {
        return totalObjectsExpected;
    }

    public void setTotalObjectsExpected(long totalObjectsExpected) {
        this.totalObjectsExpected = totalObjectsExpected;
    }

    @XmlElement(name = "BytesComplete")
    public long getBytesComplete() {
        return bytesComplete;
    }

    public void setBytesComplete(long bytesComplete) {
        this.bytesComplete = bytesComplete;
    }

    @XmlElement(name = "ObjectsComplete")
    public long getObjectsComplete() {
        return objectsComplete;
    }

    public void setObjectsComplete(long objectsComplete) {
        this.objectsComplete = objectsComplete;
    }

    @XmlElement(name = "ObjectsFailed")
    public long getObjectsFailed() {
        return objectsFailed;
    }

    public void setObjectsFailed(long objectsFailed) {
        this.objectsFailed = objectsFailed;
    }

    @XmlElement(name = "RuntimeMs")
    public long getRuntimeMs() {
        return runtimeMs;
    }

    public void setRuntimeMs(long runtimeMs) {
        this.runtimeMs = runtimeMs;
    }

    @XmlElement(name = "ActiveQueryTasks")
    public int getActiveQueryTasks() {
        return activeQueryTasks;
    }

    public void setActiveQueryTasks(int activeQueryTasks) {
        this.activeQueryTasks = activeQueryTasks;
    }

    @XmlElement(name = "ActiveSyncTasks")
    public int getActiveSyncTasks() {
        return activeSyncTasks;
    }

    public void setActiveSyncTasks(int activeSyncTasks) {
        this.activeSyncTasks = activeSyncTasks;
    }

    @XmlElement(name = "CpuTimeMs")
    public long getCpuTimeMs() {
        return cpuTimeMs;
    }

    public void setCpuTimeMs(long cpuTimeMs) {
        this.cpuTimeMs = cpuTimeMs;
    }

    @XmlElement(name = "ProcessCpuLoad")
    public double getProcessCpuLoad() {
        return processCpuLoad;
    }

    public void setProcessCpuLoad(double processCpuLoad) {
        this.processCpuLoad = processCpuLoad;
    }

    @XmlElement(name = "ProcessMemoryUsed")
    public long getProcessMemoryUsed() {
        return processMemoryUsed;
    }

    public void setProcessMemoryUsed(long processMemoryUsed) {
        this.processMemoryUsed = processMemoryUsed;
    }

    @XmlElement(name = "SourceReadRate")
    public long getSourceReadRate() {
        return sourceReadRate;
    }

    public void setSourceReadRate(long sourceReadRate) {
        this.sourceReadRate = sourceReadRate;
    }

    @XmlElement(name = "SourceWriteRate")
    public long getSourceWriteRate() {
        return sourceWriteRate;
    }

    public void setSourceWriteRate(long sourceWriteRate) {
        this.sourceWriteRate = sourceWriteRate;
    }

    @XmlElement(name = "TargetReadRate")
    public long getTargetReadRate() {
        return targetReadRate;
    }

    public void setTargetReadRate(long targetReadRate) {
        this.targetReadRate = targetReadRate;
    }

    @XmlElement(name = "TargetWriteRate")
    public long getTargetWriteRate() {
        return targetWriteRate;
    }

    public void setTargetWriteRate(long targetWriteRate) {
        this.targetWriteRate = targetWriteRate;
    }

    @XmlElement(name = "ObjectCompleteRate")
    public long getObjectCompleteRate() {
        return objectCompleteRate;
    }

    public void setObjectCompleteRate(long objectCompleteRate) {
        this.objectCompleteRate = objectCompleteRate;
    }

    @XmlElement(name = "ObjectErrorRate")
    public long getObjectErrorRate() {
        return objectErrorRate;
    }

    public void setObjectErrorRate(long objectErrorRate) {
        this.objectErrorRate = objectErrorRate;
    }

    @XmlElement(name = "RunError")
    public String getRunError() {
        return runError;
    }

    public void setRunError(String runError) {
        this.runError = runError;
    }
}
