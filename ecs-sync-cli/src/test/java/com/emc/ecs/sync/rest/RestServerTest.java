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

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.service.SyncJobService;
import com.emc.ecs.sync.test.ByteAlteringFilter;
import com.emc.ecs.sync.test.DelayFilter;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogManager;

public class RestServerTest {
    private static final Logger log = LoggerFactory.getLogger(RestServerTest.class);

    private static final String HOST = "localhost";
    private static final int PORT = RestServer.DEFAULT_PORT;
    private static final URI endpoint = UriBuilder.fromUri("http://" + HOST).port(PORT).build();
    private static final String XML = "application/xml";

    private RestServer restServer;
    private Client client;

    @BeforeEach
    public void startRestServer() {
        // first, hush up the JDK logger (why does this default to INFO??)
        LogManager.getLogManager().getLogger("").setLevel(Level.WARNING);
        restServer = new RestServer(endpoint.getHost(), endpoint.getPort());
        restServer.start();
    }

    @AfterEach
    public void stopRestServer() throws InterruptedException {
        if (restServer != null) restServer.stop(0);
        restServer = null;

        // wait for all jobs to stop to prevent leakage into other tests
        for (JobInfo jobInfo : SyncJobService.getInstance().getAllJobs().getJobs()) {
            while (!SyncJobService.getInstance().getJobControl(jobInfo.getJobId()).getStatus().isFinalState())
                Thread.sleep(500);
            SyncJobService.getInstance().deleteJob(jobInfo.getJobId(), false);
        }
    }

    @BeforeEach
    public void createClient() {
        ClientConfig cc = new DefaultClientConfig();
        cc.getSingletons().add(new PluginResolver());
        client = Client.create(cc);
    }

    @Test
    public void testAutoPort() throws Exception {
        List<HttpServer> httpServers = new ArrayList<>();
        List<RestServer> restServers = new ArrayList<>();
        try {
            // one port is already used by restServer
            for (int i = 1; i <= 3; i++) {
                httpServers.add(HttpServer.create(new InetSocketAddress(HOST, PORT + i), 0));
            }

            // this should leave one port available within the max 5 bind attempts
            // the following should succeed
            RestServer restServer = new RestServer(HOST, PORT);
            restServer.setAutoPortEnabled(true);
            restServer.start();
            restServers.add(restServer);

            // now we have used 5 ports, so the max bind attempts should be reached
            // the following should fail
            try {
                restServer = new RestServer(HOST, PORT);
                restServer.setAutoPortEnabled(true);
                restServer.start();
                restServers.add(restServer);
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertEquals("Exceeded maximum bind attempts", e.getCause().getMessage());
            }
        } finally {
            for (HttpServer httpServer : httpServers) {
                httpServer.stop(0);
            }
            for (RestServer restServer : restServers) {
                restServer.stop(0);
            }
        }
    }

    @Test
    public void testServerStartup() {
        Assertions.assertNotNull(client.resource(endpoint).path("/job").get(JobList.class));
    }

    @Test
    public void testJobNotFound() {
        try {
            client.resource(endpoint).path("/job/1").get(JobControl.class);
            Assertions.fail("server should return a 404");
        } catch (UniformInterfaceException e) {
            Assertions.assertEquals(404, e.getResponse().getStatus());
        }
    }

    @Test
    public void testGetJob() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withObjectCount(10).withMaxSize(10240).withDiscardData(false));
        syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));

        // create sync job
        ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
        String jobId = response.getHeaders().getFirst("x-emc-job-id");
        try {
            Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
            response.close(); // must close all responses

            // get job
            SyncConfig syncConfig2 = client.resource(endpoint).path("/job/" + jobId).get(SyncConfig.class);

            Assertions.assertNotNull(syncConfig2);
            Assertions.assertEquals(syncConfig, syncConfig2);

            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }
        } finally {
            // delete job
            response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
            if (response.getStatus() != 200)
                log.warn("could not delete job: {}", response.getEntity(String.class));
            response.close(); // must close all responses
        }
    }

    @Test
    public void testListJobs() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withObjectCount(10).withMaxSize(10240).withDiscardData(false));
        syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));

        // create 3 sync jobs
        ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
        Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
        String jobId1 = response.getHeaders().getFirst("x-emc-job-id");
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
        Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
        String jobId2 = response.getHeaders().getFirst("x-emc-job-id");
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
        Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
        String jobId3 = response.getHeaders().getFirst("x-emc-job-id");
        response.close(); // must close all responses

        // get job list
        JobList jobList = client.resource(endpoint).path("/job").get(JobList.class);

        Assertions.assertNotNull(jobList);
        Assertions.assertEquals(3, jobList.getJobs().size());
        Assertions.assertEquals(Integer.valueOf(jobId1), jobList.getJobs().get(0).getJobId());
        Assertions.assertEquals(Integer.valueOf(jobId2), jobList.getJobs().get(1).getJobId());
        Assertions.assertEquals(Integer.valueOf(jobId3), jobList.getJobs().get(2).getJobId());

        // wait for jobs to complete
        while (!client.resource(endpoint).path("/job/" + jobId1 + "/control").get(JobControl.class).getStatus().isFinalState()
                || !client.resource(endpoint).path("/job/" + jobId2 + "/control").get(JobControl.class).getStatus().isFinalState()
                || !client.resource(endpoint).path("/job/" + jobId3 + "/control").get(JobControl.class).getStatus().isFinalState()) {
            Thread.sleep(1000);
        }

        // delete jobs
        response = client.resource(endpoint).path("/job/" + jobId1).delete(ClientResponse.class);
        Assertions.assertEquals(200, response.getStatus(), response.getEntity(String.class));
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job/" + jobId2).delete(ClientResponse.class);
        Assertions.assertEquals(200, response.getStatus(), response.getEntity(String.class));
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job/" + jobId3).delete(ClientResponse.class);
        Assertions.assertEquals(200, response.getStatus(), response.getEntity(String.class));
        response.close(); // must close all responses
    }

    @Test
    public void testCreateDelete() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withObjectCount(10).withMaxSize(10240).withDiscardData(false));
        syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));

        // create sync job
        ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
        String jobIdStr = response.getHeaders().getFirst("x-emc-job-id");
        try {
            Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
            response.close(); // must close all responses

            Assertions.assertNotNull(jobIdStr);
            int jobId = Integer.parseInt(jobIdStr);

            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }

            // get status (should be complete)
            JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assertions.assertNotNull(jobControl);
            Assertions.assertEquals(JobControlStatus.Complete, jobControl.getStatus());
        } finally {
            // delete job
            response = client.resource(endpoint).path("/job/" + jobIdStr).delete(ClientResponse.class);
            if (response.getStatus() != 200)
                log.warn("could not delete job: {}", response.getEntity(String.class));
            response.close(); // must close all responses
        }
    }

    @Test
    public void testPauseResume() throws Exception {
        try {
            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setSource(new TestConfig().withObjectCount(10).withMaxSize(10240).withDiscardData(false));
            syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));
            syncConfig.setFilters(Collections.singletonList(new DelayFilter.DelayConfig().withDelayMs(100)));
            syncConfig.withOptions(new SyncOptions().withThreadCount(2));

            // create sync job
            ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");

            Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
            response.close(); // must close all responses

            // wait for sync to start
            while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                Thread.sleep(200);
            }

            // pause job
            client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Paused, 0));

            // wait a tick
            Thread.sleep(1000);

            // get control status
            JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assertions.assertNotNull(jobControl);
            Assertions.assertEquals(JobControlStatus.Paused, jobControl.getStatus());
            Assertions.assertEquals(2, jobControl.getThreadCount());

            // get baseline for progress comparison to make sure we're really paused
            SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);

            // wait a tick
            Thread.sleep(1000);

            // compare against baseline to make sure nothing's changed
            SyncProgress progress2 = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
            Assertions.assertEquals(progress.getObjectsComplete(), progress2.getObjectsComplete());
            Assertions.assertEquals(progress.getBytesComplete(), progress2.getBytesComplete());

            // resume
            client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Running, 0));

            // get control status
            jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assertions.assertNotNull(jobControl);
            Assertions.assertEquals(JobControlStatus.Running, jobControl.getStatus());
            Assertions.assertEquals(2, jobControl.getThreadCount());

            // bump threads to speed up completion
            client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Running, 32));
            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }

            // get control status
            jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assertions.assertNotNull(jobControl);
            Assertions.assertEquals(JobControlStatus.Complete, jobControl.getStatus());

            progress2 = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
            Assertions.assertTrue(progress.getObjectsComplete() < progress2.getObjectsComplete());
            Assertions.assertTrue(progress.getBytesComplete() < progress2.getBytesComplete());

            // delete job
            response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
            Assertions.assertEquals(200, response.getStatus(), response.getEntity(String.class));
            response.close(); // must close all responses
        } catch (UniformInterfaceException e) {
            log.error(e.getResponse().getEntity(String.class));
            throw e;
        }
    }

    @Test
    public void testChangeThreadCount() throws Exception {
        try {
            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setSource(new TestConfig().withObjectCount(130).withMaxSize(10240).withDiscardData(false));
            syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));
            syncConfig.setFilters(Collections.singletonList(new DelayFilter.DelayConfig().withDelayMs(100)));
            syncConfig.withOptions(new SyncOptions().withThreadCount(1));

            // create sync job
            ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");

            // wait for sync to start
            while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                Thread.sleep(200);
            }

            try {
                Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
                response.close(); // must close all responses

                // wait a tick
                Thread.sleep(1000);

                // 1 threads can process about 10 objects in 1 second with 100ms delay
                // we will give 200ms margin for execution, so 8 <= # <= 12
                SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                long totalCount = progress.getObjectsComplete();
                String message = "completed count = " + totalCount;
                Assertions.assertTrue(totalCount >= 8, message);
                Assertions.assertTrue(totalCount <= 12, message);

                // up threads to 10
                client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Running, 10));

                // wait a tick
                Thread.sleep(1000);

                // 10 threads can process about 100 objects in 1 second with 100ms delay
                // we will give 200ms margin for execution, so 80 <= # <= 120
                // (totalCount already processed)
                progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                message = "previous count = " + totalCount + ", completed count = " + progress.getObjectsComplete();
                Assertions.assertTrue(progress.getObjectsComplete() >= 80 + totalCount, message);
                Assertions.assertTrue(progress.getObjectsComplete() <= 120 + totalCount, message);

                // lower threads to 2
                client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Running, 2));
                Thread.sleep(200); // give thread pool a chance to stabilize
                totalCount = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class).getObjectsComplete();

                // wait a tick
                Thread.sleep(1000);

                // 2 threads can process about 20 objects in 1 second with 100ms delay
                // we will give 200ms margin for execution, so 16 <= # <= 24
                // (totalCount already processed)
                progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                message = "previous count = " + totalCount + ", completed count = " + progress.getObjectsComplete();
                Assertions.assertTrue(progress.getObjectsComplete() >= 16 + totalCount, message);
                Assertions.assertTrue(progress.getObjectsComplete() <= 24 + totalCount, message);

                // bump threads to speed up completion
                client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Running, 32));
                // wait for job to complete
                while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                    Thread.sleep(1000);
                }

                JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
                Assertions.assertEquals(JobControlStatus.Complete, jobControl.getStatus());
            } finally {
                // delete job
                response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
                if (response.getStatus() != 200)
                    log.warn("could not delete job: {}", response.getEntity(String.class));
                response.close(); // must close all responses
            }
        } catch (UniformInterfaceException e) {
            log.error(e.getResponse().getEntity(String.class));
            throw e;
        }
    }

    @Test
    public void testProgress() throws Exception {
        try {
            int threads = 16;

            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setSource(new TestConfig().withObjectCount(500).withMaxSize(10240).withChanceOfChildren(0).withDiscardData(false));
            syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));
            syncConfig.setFilters(Collections.singletonList(new DelayFilter.DelayConfig().withDelayMs(100)));
            syncConfig.withOptions(new SyncOptions().withThreadCount(threads));

            // create sync job
            ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");

            Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
            response.close(); // must close all responses

            // wait for sync to start
            while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                Thread.sleep(500);
            }
            Thread.sleep(500);

            SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
            Assertions.assertEquals(JobControlStatus.Running, progress.getStatus());
            Assertions.assertTrue(progress.getTotalObjectsExpected() > 100);
            Assertions.assertTrue(progress.getTotalBytesExpected() > 100 * 5120);
            Assertions.assertTrue(progress.getObjectsComplete() > 0);
            Assertions.assertTrue(progress.getBytesComplete() > 0);
            Assertions.assertEquals(0, progress.getObjectsFailed());
            Assertions.assertEquals(progress.getActiveQueryTasks(), 0);
            Assertions.assertTrue(Math.abs(progress.getActiveSyncTasks() - threads) < 2);
            Assertions.assertTrue(progress.getRuntimeMs() > 500);

            // bump threads to speed up completion
            client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Running, 32));
            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }

            JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assertions.assertEquals(JobControlStatus.Complete, jobControl.getStatus());

            // delete job
            response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
            Assertions.assertEquals(200, response.getStatus(), response.getEntity(String.class));
            response.close(); // must close all responses
        } catch (UniformInterfaceException e) {
            log.error(e.getResponse().getEntity(String.class));
            throw e;
        }
    }

    @Test
    public void testTerminate() throws Exception {
        try {
            int threads = 4;

            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setSource(new TestConfig().withObjectCount(100).withMaxSize(10240).withDiscardData(false));
            syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));
            syncConfig.setFilters(Collections.singletonList(new DelayFilter.DelayConfig().withDelayMs(100)));
            syncConfig.withOptions(new SyncOptions().withThreadCount(threads));

            // create sync job
            ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");
            try {
                Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
                response.close(); // must close all responses

                // wait for sync to start
                while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                    Thread.sleep(500);
                }

                // stop the job
                client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Stopped, 4));

                // wait a tick
                Thread.sleep(1000);

                // active tasks should clear out after 1 second and the queue should be cleared, so the job should terminate
                JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
                Assertions.assertEquals(JobControlStatus.Stopped, jobControl.getStatus());
            } finally {
                // delete job
                response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
                if (response.getStatus() != 200)
                    log.warn("could not delete job: {}", response.getEntity(String.class));
                response.close(); // must close all responses
            }
        } catch (UniformInterfaceException e) {
            log.error(e.getResponse().getEntity(String.class));
            throw e;
        }
    }

    @Test
    public void testTerminateWhilePaused() throws Exception {
        try {
            int threads = 2;

            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setSource(new TestConfig().withObjectCount(100).withMaxSize(10240).withDiscardData(false));
            syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));
            syncConfig.setFilters(Collections.singletonList(new DelayFilter.DelayConfig().withDelayMs(100)));
            syncConfig.withOptions(new SyncOptions().withThreadCount(threads));

            // create sync job
            ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");
            try {
                Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
                response.close(); // must close all responses

                // wait for sync to start
                while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                    Thread.sleep(500);
                }

                // pause the job
                client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Paused, 4));

                // wait a tick for tasks to clear out
                Thread.sleep(1000);

                SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);

                // stop the job
                client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Stopped, 4));

                // wait a tick for monitor loop to exit
                Thread.sleep(1000);

                // job should terminate right away
                JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
                Assertions.assertEquals(JobControlStatus.Stopped, jobControl.getStatus());

                // no additional completions or errors should have occurred
                SyncProgress progress2 = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                Assertions.assertEquals(progress.getObjectsComplete(), progress2.getObjectsComplete());
                Assertions.assertEquals(progress.getObjectsFailed(), progress2.getObjectsFailed());
            } finally {
                // delete job
                response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
                if (response.getStatus() != 200)
                    log.warn("could not delete job: {}", response.getEntity(String.class));
                response.close(); // must close all responses
            }
        } catch (UniformInterfaceException e) {
            log.error(e.getResponse().getEntity(String.class));
            throw e;
        }
    }

    @Test
    public void testReports() throws Exception {
        try {
            File tempDb = File.createTempFile("temp.db", null);
            tempDb.deleteOnExit();

            SyncConfig syncConfig = new SyncConfig();
            syncConfig.setSource(new TestConfig().withObjectCount(100).withChanceOfChildren(0).withDiscardData(false));
            syncConfig.setTarget(new TestConfig().withDiscardData(false));
            syncConfig.setFilters(Arrays.asList(new ByteAlteringFilter.ByteAlteringConfig(), new DelayFilter.DelayConfig().withDelayMs(400)));
            syncConfig.withOptions(new SyncOptions().withVerify(true).withThreadCount(20).withRetryAttempts(1).withDbFile(tempDb.getAbsolutePath()));

            // create job
            ClientResponse response = client.resource(endpoint).path("/job").type(XML).put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");
            try {
                Assertions.assertEquals(201, response.getStatus(), response.getEntity(String.class));
                response.close(); // must close all responses

                // wait to start
                while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                    Thread.sleep(500);
                }

                // wait a bit so at least some objects complete
                while (client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class).getObjectsComplete() < 10) {
                    Thread.sleep(200);
                }

                // half should fail validation
                SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                Assertions.assertTrue(progress.getObjectsAwaitingRetry() > 0);

                // get retry report
                response = client.resource(endpoint).path("/job/" + jobId + "/retries.csv").get(ClientResponse.class);
                CSVParser parser = CSVFormat.EXCEL.parse(new InputStreamReader(response.getEntityInputStream()));

                Assertions.assertTrue(parser.getRecords().size() > 0);

                // stop the job
                client.resource(endpoint).path("/job/" + jobId + "/control").type(XML).post(new JobControl(JobControlStatus.Stopped, 20));

                // wait a tick for monitor loop to exit
                Thread.sleep(1400);

                // get error report
                response = client.resource(endpoint).path("/job/" + jobId + "/errors.csv").get(ClientResponse.class);
                parser = CSVFormat.EXCEL.parse(new InputStreamReader(response.getEntityInputStream()));

                Assertions.assertTrue(parser.getRecords().size() > 0);

                // get all object report
                response = client.resource(endpoint).path("/job/" + jobId + "/all-objects-report.csv").get(ClientResponse.class);
                parser = CSVFormat.EXCEL.parse(new InputStreamReader(response.getEntityInputStream()));

                Assertions.assertTrue(parser.getRecords().size() > 0);
            } finally {
                // delete job
                response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
                if (response.getStatus() != 200)
                    log.warn("could not delete job: {}", response.getEntity(String.class));
                response.close(); // must close all responses
            }
        } catch (UniformInterfaceException e) {
            log.error(e.getResponse().getEntity(String.class));
            throw e;
        }
    }
}
