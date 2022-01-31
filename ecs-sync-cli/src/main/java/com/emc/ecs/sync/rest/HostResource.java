/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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

import com.emc.ecs.sync.EcsSyncCli;
import com.emc.ecs.sync.LoggingUtil;
import com.sun.jersey.spi.resource.Singleton;
import com.sun.management.OperatingSystemMXBean;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.lang.management.ManagementFactory;

@Singleton
@Path("/host")
public class HostResource {
    @GET
    @Produces(MediaType.APPLICATION_XML)
    public HostInfo get() {
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        HostInfo hostInfo = new HostInfo();
        hostInfo.setEcsSyncVersion(EcsSyncCli.VERSION == null ? "Unreleased" : EcsSyncCli.VERSION);
        hostInfo.setHostCpuCount(Runtime.getRuntime().availableProcessors());
        hostInfo.setHostCpuLoad(osBean.getSystemCpuLoad());
        hostInfo.setHostTotalMemory(osBean.getTotalPhysicalMemorySize());
        hostInfo.setHostMemoryUsed(hostInfo.getHostTotalMemory() - osBean.getFreePhysicalMemorySize());
        hostInfo.setLogLevel(LoggingUtil.getRootLogLevel());

        return hostInfo;
    }

    @POST
    @Path("logging")
    public Response setLogLevel(@QueryParam("level") LogLevel logLevel) {
        LoggingUtil.setRootLogLevel(logLevel);
        return Response.status(Response.Status.OK).build();
    }
}
