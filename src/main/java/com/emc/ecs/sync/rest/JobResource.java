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

import com.emc.ecs.sync.service.SyncJobService;
import com.emc.ecs.sync.util.ConfigurationException;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.api.client.ClientResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;

@Path("job")
public class JobResource {
    @GET
    @Produces(MediaType.APPLICATION_XML)
    public JobList list() {
        return SyncJobService.getInstance().getAllJobs();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_XML)
    public Response put(SyncConfig syncConfig) {
        try {
            int jobId = SyncJobService.getInstance().createJob(syncConfig);
            return Response.created(UriBuilder.fromPath("/" + jobId).build()).header("x-emc-job-id", jobId).build();
        } catch (ConfigurationException e) { // config is bad
            return Response.status(Response.Status.BAD_REQUEST).type(MediaType.TEXT_PLAIN).entity(e.toString()).build();
        } catch (UnsupportedOperationException e) { // can't create any more jobs
            return Response.status(Response.Status.CONFLICT).type(MediaType.TEXT_PLAIN).entity(e.toString()).build();
        }
    }

    @GET
    @Path("{jobId}")
    @Produces(MediaType.APPLICATION_XML)
    public SyncConfig get(@PathParam("jobId") int jobId) {
        SyncConfig syncConfig = SyncJobService.getInstance().getJob(jobId);
        if (syncConfig == null) throw new NotFoundException(); // job not found
        return syncConfig;
    }

    @DELETE
    @Path("{jobId}")
    public Response delete(@PathParam("jobId") int jobId) {
        try {
            SyncJobService.getInstance().deleteJob(jobId);
            return Response.ok().build();
        } catch (IllegalArgumentException e) { // job not found
            throw new NotFoundException(e.getMessage());
        } catch (UnsupportedOperationException e) { // job is running or paused (can't be deleted)
            return Response.status(ClientResponse.Status.CONFLICT).type(MediaType.TEXT_PLAIN).entity(e.toString()).build();
        }
    }

    @GET
    @Path("{jobId}/control")
    @Produces(MediaType.APPLICATION_XML)
    public JobControl getControl(@PathParam("jobId") int jobId) {
        JobControl jobControl = SyncJobService.getInstance().getJobControl(jobId);
        if (jobControl == null) throw new NotFoundException(); // job not found
        return jobControl;
    }

    @POST
    @Path("{jobId}/control")
    @Consumes(MediaType.APPLICATION_XML)
    public Response setControl(@PathParam("jobId") int jobId, JobControl jobControl) {
        try {
            SyncJobService.getInstance().setJobControl(jobId, jobControl);
            return Response.ok().build();
        } catch (IllegalArgumentException e) { // job not found
            throw new NotFoundException(e.getMessage());
        } catch (IllegalStateException e) { // job is stopped and cannot be restarted
            return Response.status(ClientResponse.Status.CONFLICT).type(MediaType.TEXT_PLAIN).entity(e.toString()).build();
        }
    }

    @GET
    @Path("{jobId}/progress")
    @Produces(MediaType.APPLICATION_XML)
    public SyncProgress getProgress(@PathParam("jobId") int jobId) {
        SyncProgress syncProgress = SyncJobService.getInstance().getProgress(jobId);
        if (syncProgress == null) throw new NotFoundException(); // job not found
        return syncProgress;
    }

    @GET
    @Path("{jobId}/errors.csv")
    @Produces("text/csv")
    public Response getErrors(@PathParam("jobId") int jobId) throws IOException {
        if (!SyncJobService.getInstance().jobExists(jobId)) throw new NotFoundException(); // job not found

        ErrorStreamWriter streamWriter = new ErrorStreamWriter(SyncJobService.getInstance().getSyncErrors(jobId));

        Thread streamWriterThread = new Thread(streamWriter);
        streamWriterThread.setDaemon(true);
        streamWriterThread.start();

        return Response.ok(streamWriter.getReadStream()).build();
    }
}
