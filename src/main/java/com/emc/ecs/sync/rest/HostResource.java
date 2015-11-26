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
package com.emc.ecs.sync.rest;

import com.emc.ecs.sync.bean.HostInfo;
import com.sun.management.OperatingSystemMXBean;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.lang.management.ManagementFactory;

@Path("/host")
public class HostResource {
    @GET
    @Produces(MediaType.APPLICATION_XML)
    public HostInfo get() {
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        HostInfo hostInfo = new HostInfo();
        hostInfo.setHostCpuCount(Runtime.getRuntime().availableProcessors());
        hostInfo.setHostCpuLoad(osBean.getSystemCpuLoad());
        hostInfo.setHostTotalMemory(osBean.getTotalPhysicalMemorySize());
        hostInfo.setHostMemoryUsed(hostInfo.getHostTotalMemory() - osBean.getFreePhysicalMemorySize());

        return hostInfo;
    }
}
