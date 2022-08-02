/*
 * Copyright (c) 2015-2017 Dell Inc. or its subsidiaries. All Rights Reserved.
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

@XmlRootElement
public class HostInfo {
    private String ecsSyncVersion;
    private int hostCpuCount;
    private double hostCpuLoad;
    private long hostMemoryUsed;
    private long hostTotalMemory;
    private LogLevel logLevel;

    public String getEcsSyncVersion() {
        return ecsSyncVersion;
    }

    public void setEcsSyncVersion(String ecsSyncVersion) {
        this.ecsSyncVersion = ecsSyncVersion;
    }

    public int getHostCpuCount() {
        return hostCpuCount;
    }

    public void setHostCpuCount(int hostCpuCount) {
        this.hostCpuCount = hostCpuCount;
    }

    public double getHostCpuLoad() {
        return hostCpuLoad;
    }

    public void setHostCpuLoad(double hostCpuLoad) {
        this.hostCpuLoad = hostCpuLoad;
    }

    public long getHostMemoryUsed() {
        return hostMemoryUsed;
    }

    public void setHostMemoryUsed(long hostMemoryUsed) {
        this.hostMemoryUsed = hostMemoryUsed;
    }

    public long getHostTotalMemory() {
        return hostTotalMemory;
    }

    public void setHostTotalMemory(long hostTotalMemory) {
        this.hostTotalMemory = hostTotalMemory;
    }

    public LogLevel getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(LogLevel logLevel) {
        this.logLevel = logLevel;
    }
}
