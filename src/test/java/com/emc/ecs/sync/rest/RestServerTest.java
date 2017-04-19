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

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.config.storage.TestConfig;
import com.emc.ecs.sync.test.DelayFilter;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogManager;

public class RestServerTest {
    private static final Logger log = LoggerFactory.getLogger(RestServerTest.class);

    private static final String HOST = "localhost";
    private static final int PORT = 9200;
    private static URI endpoint = UriBuilder.fromUri("http://" + HOST).port(PORT).build();

    private RestServer restServer;
    private Client client;

    @Before
    public void startRestServer() {
        // first, hush up the JDK logger (why does this default to INFO??)
        LogManager.getLogManager().getLogger("").setLevel(Level.WARNING);
        restServer = new RestServer(endpoint.getHost(), endpoint.getPort());
        restServer.start();
    }

    @After
    public void stopRestServer() {
        if (restServer != null) restServer.stop(0);
        restServer = null;
    }

    @Before
    public void createClient() throws Exception {
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
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals("Exceeded maximum bind attempts", e.getCause().getMessage());
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
    public void testServerStartup() throws Exception {
        Assert.assertNotNull(client.resource(endpoint).path("/job").get(JobList.class));
    }

    @Test
    public void testJobNotFound() throws Exception {
        try {
            client.resource(endpoint).path("/job/1").get(JobControl.class);
            Assert.fail("server should return a 404");
        } catch (UniformInterfaceException e) {
            Assert.assertEquals(404, e.getResponse().getStatus());
        }
    }

    @Test
    public void testGetJob() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withObjectCount(10).withMaxSize(10240).withDiscardData(false));
        syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));

        // create sync job
        ClientResponse response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
        String jobId = response.getHeaders().getFirst("x-emc-job-id");
        try {
            Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
            response.close(); // must close all responses

            // get job
            SyncConfig syncConfig2 = client.resource(endpoint).path("/job/" + jobId).get(SyncConfig.class);

            Assert.assertNotNull(syncConfig2);
            Assert.assertEquals(syncConfig, syncConfig2);

            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }
        } finally {
            // delete job
            response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
            Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
            response.close(); // must close all responses
        }
    }

    @Test
    public void testListJobs() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withObjectCount(10).withMaxSize(10240).withDiscardData(false));
        syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));

        // create 3 sync jobs
        ClientResponse response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
        Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
        String jobId1 = response.getHeaders().getFirst("x-emc-job-id");
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
        Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
        String jobId2 = response.getHeaders().getFirst("x-emc-job-id");
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
        Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
        String jobId3 = response.getHeaders().getFirst("x-emc-job-id");
        response.close(); // must close all responses

        // get job list
        JobList jobList = client.resource(endpoint).path("/job").get(JobList.class);

        Assert.assertNotNull(jobList);
        Assert.assertEquals(3, jobList.getJobs().size());
        Assert.assertEquals(new Integer(jobId1), jobList.getJobs().get(0).getJobId());
        Assert.assertEquals(new Integer(jobId2), jobList.getJobs().get(1).getJobId());
        Assert.assertEquals(new Integer(jobId3), jobList.getJobs().get(2).getJobId());

        // wait for jobs to complete
        while (!client.resource(endpoint).path("/job/" + jobId1 + "/control").get(JobControl.class).getStatus().isFinalState()
                && !client.resource(endpoint).path("/job/" + jobId2 + "/control").get(JobControl.class).getStatus().isFinalState()
                && !client.resource(endpoint).path("/job/" + jobId3 + "/control").get(JobControl.class).getStatus().isFinalState()) {
            Thread.sleep(1000);
        }

        // delete jobs
        response = client.resource(endpoint).path("/job/" + jobId1).delete(ClientResponse.class);
        Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job/" + jobId2).delete(ClientResponse.class);
        Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
        response.close(); // must close all responses

        response = client.resource(endpoint).path("/job/" + jobId3).delete(ClientResponse.class);
        Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
        response.close(); // must close all responses
    }

    @Test
    public void testCreateDelete() throws Exception {
        SyncConfig syncConfig = new SyncConfig();
        syncConfig.setSource(new TestConfig().withObjectCount(10).withMaxSize(10240).withDiscardData(false));
        syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));

        // create sync job
        ClientResponse response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
        String jobIdStr = response.getHeaders().getFirst("x-emc-job-id");
        try {
            Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
            response.close(); // must close all responses

            Assert.assertNotNull(jobIdStr);
            int jobId = Integer.parseInt(jobIdStr);

            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }

            // get status (should be complete)
            JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assert.assertNotNull(jobControl);
            Assert.assertEquals(JobControlStatus.Complete, jobControl.getStatus());
        } finally {
            // delete job
            response = client.resource(endpoint).path("/job/" + jobIdStr).delete(ClientResponse.class);
            Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
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
            ClientResponse response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");

            Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
            response.close(); // must close all responses

            // wait for sync to start
            while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                Thread.sleep(200);
            }

            // pause job
            client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Paused, 0));

            // wait a tick
            Thread.sleep(1000);

            // get control status
            JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assert.assertNotNull(jobControl);
            Assert.assertEquals(JobControlStatus.Paused, jobControl.getStatus());
            Assert.assertEquals(2, jobControl.getThreadCount());

            // get baseline for progress comparison to make sure we're really paused
            SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);

            // wait a tick
            Thread.sleep(1000);

            // compare against baseline to make sure nothing's changed
            SyncProgress progress2 = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
            Assert.assertEquals(progress.getObjectsComplete(), progress2.getObjectsComplete());
            Assert.assertEquals(progress.getBytesComplete(), progress2.getBytesComplete());

            // resume
            client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Running, 0));

            // get control status
            jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assert.assertNotNull(jobControl);
            Assert.assertEquals(JobControlStatus.Running, jobControl.getStatus());
            Assert.assertEquals(2, jobControl.getThreadCount());

            // bump threads to speed up completion
            client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Running, 32));
            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }

            // get control status
            jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assert.assertNotNull(jobControl);
            Assert.assertEquals(JobControlStatus.Complete, jobControl.getStatus());

            progress2 = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
            Assert.assertTrue(progress.getObjectsComplete() < progress2.getObjectsComplete());
            Assert.assertTrue(progress.getBytesComplete() < progress2.getBytesComplete());

            // delete job
            response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
            Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
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
            ClientResponse response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");
            try {
                Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
                response.close(); // must close all responses

                // wait a tick
                Thread.sleep(1000);

                // 1 threads can process max 10 objects in 1 second with 100ms delay and min 5
                // so 5 < # < 10
                SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                long totalCount = progress.getObjectsComplete();
                Assert.assertTrue(totalCount >= 5);
                Assert.assertTrue(totalCount <= 10);

                // up threads to 10
                client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Running, 10));

                // wait a tick
                Thread.sleep(1000);

                // 10 threads can process max 100 objects in 1 second with 100ms delay and min 60
                // (totalCount already processed)
                progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                Assert.assertTrue(progress.getObjectsComplete() >= 60 + totalCount);
                Assert.assertTrue(progress.getObjectsComplete() <= 100 + totalCount);

                // lower threads to 2
                client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Running, 2));
                Thread.sleep(300);
                totalCount = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class).getObjectsComplete();

                // wait a tick
                Thread.sleep(1000);

                // 2 threads can process max 20 objects in 1 second with 100ms delay and min 15
                // (totalCount already processed)
                progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
                Assert.assertTrue(progress.getObjectsComplete() >= 15 + totalCount);
                Assert.assertTrue(progress.getObjectsComplete() <= 20 + totalCount);

                // bump threads to speed up completion
                client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Running, 32));
                // wait for job to complete
                while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                    Thread.sleep(1000);
                }

                JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
                Assert.assertEquals(JobControlStatus.Complete, jobControl.getStatus());
            } finally {
                // delete job
                response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
                Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
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
            syncConfig.setSource(new TestConfig().withObjectCount(80).withMaxSize(10240).withMaxDepth(4).withDiscardData(false));
            syncConfig.setTarget(new TestConfig().withReadData(true).withDiscardData(false));
            syncConfig.setFilters(Collections.singletonList(new DelayFilter.DelayConfig().withDelayMs(100)));
            syncConfig.withOptions(new SyncOptions().withThreadCount(threads));

            // create sync job
            ClientResponse response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");

            Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
            response.close(); // must close all responses

            // wait for sync to start
            while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                Thread.sleep(500);
            }
            Thread.sleep(500);

            SyncProgress progress = client.resource(endpoint).path("/job/" + jobId + "/progress").get(SyncProgress.class);
            Assert.assertEquals(JobControlStatus.Running, progress.getStatus());
            Assert.assertTrue(progress.getTotalObjectsExpected() > 100);
            Assert.assertTrue(progress.getTotalBytesExpected() > 100 * 5120);
            Assert.assertTrue(progress.getObjectsComplete() > 0);
            Assert.assertTrue(progress.getBytesComplete() > 0);
            Assert.assertEquals(0, progress.getObjectsFailed());
            Assert.assertEquals(progress.getActiveQueryTasks(), 0);
            Assert.assertTrue(Math.abs(progress.getActiveSyncTasks() - threads) < 2);
            Assert.assertTrue(progress.getRuntimeMs() > 500);

            // bump threads to speed up completion
            client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Running, 32));
            // wait for job to complete
            while (!client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus().isFinalState()) {
                Thread.sleep(1000);
            }

            JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
            Assert.assertEquals(JobControlStatus.Complete, jobControl.getStatus());

            // delete job
            response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
            Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
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
            ClientResponse response = client.resource(endpoint).path("/job").put(ClientResponse.class, syncConfig);
            String jobId = response.getHeaders().getFirst("x-emc-job-id");
            try {
                Assert.assertEquals(response.getEntity(String.class), 201, response.getStatus());
                response.close(); // must close all responses

                // wait for sync to start
                while (client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class).getStatus() == JobControlStatus.Initializing) {
                    Thread.sleep(500);
                }

                // stop the job
                client.resource(endpoint).path("/job/" + jobId + "/control").post(new JobControl(JobControlStatus.Stopped, 4));

                // wait a tick
                Thread.sleep(1000);

                // active tasks should clear out after 1 second and the queue should be cleared, so the job should terminate
                JobControl jobControl = client.resource(endpoint).path("/job/" + jobId + "/control").get(JobControl.class);
                Assert.assertEquals(JobControlStatus.Stopped, jobControl.getStatus());
            } finally {
                // delete job
                response = client.resource(endpoint).path("/job/" + jobId).delete(ClientResponse.class);
                Assert.assertEquals(response.getEntity(String.class), 200, response.getStatus());
                response.close(); // must close all responses
            }
        } catch (UniformInterfaceException e) {
            log.error(e.getResponse().getEntity(String.class));
            throw e;
        }
    }
}
