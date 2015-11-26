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

@XmlRootElement(name = "HostInfo")
@XmlType(propOrder = {"hostCpuCount", "hostCpuLoad", "hostMemoryUsed", "hostTotalMemory"})
public class HostInfo {
    private int hostCpuCount;
    private double hostCpuLoad;
    private long hostMemoryUsed;
    private long hostTotalMemory;

    @XmlElement(name = "HostCpuCount")
    public int getHostCpuCount() {
        return hostCpuCount;
    }

    public void setHostCpuCount(int hostCpuCount) {
        this.hostCpuCount = hostCpuCount;
    }

    @XmlElement(name = "HostCpuLoad")
    public double getHostCpuLoad() {
        return hostCpuLoad;
    }

    public void setHostCpuLoad(double hostCpuLoad) {
        this.hostCpuLoad = hostCpuLoad;
    }

    @XmlElement(name = "HostMemoryUsed")
    public long getHostMemoryUsed() {
        return hostMemoryUsed;
    }

    public void setHostMemoryUsed(long hostMemoryUsed) {
        this.hostMemoryUsed = hostMemoryUsed;
    }

    @XmlElement(name = "HostTotalMemory")
    public long getHostTotalMemory() {
        return hostTotalMemory;
    }

    public void setHostTotalMemory(long hostTotalMemory) {
        this.hostTotalMemory = hostTotalMemory;
    }
}
