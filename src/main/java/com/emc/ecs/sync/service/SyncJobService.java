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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.SyncStats;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.rest.*;
import com.emc.ecs.sync.util.SyncUtil;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncJobService {
    private static final Logger log = LoggerFactory.getLogger(SyncJobService.class);

    public static final int MAX_JOBS = 10; // maximum of 10 sync jobs per JVM process

    private static SyncJobService instance;

    public static synchronized SyncJobService getInstance() {
        if (instance == null) {
            instance = new SyncJobService();
        }
        return instance;
    }

    private String dbConnectString;
    private Map<Integer, EcsSync> syncCache = new TreeMap<>();
    private Map<Integer, SyncConfig> configCache = new TreeMap<>();
    private AtomicInteger nextJobId = new AtomicInteger(0);

    public JobList getAllJobs() {
        JobList jobList = new JobList();
        for (Map.Entry<Integer, EcsSync> entry : syncCache.entrySet()) {
            jobList.getJobs().add(new JobInfo(entry.getKey(), getJobStatus(entry.getValue()),
                    configCache.get(entry.getKey()), getProgress(entry.getKey())));
        }
        return jobList;
    }

    public boolean jobExists(int jobId) {
        return syncCache.containsKey(jobId);
    }

    public int createJob(SyncConfig syncConfig) {
        if (syncCache.size() >= MAX_JOBS)
            throw new UnsupportedOperationException("the maximum number of jobs (" + MAX_JOBS + ") has been reached");

        int jobId = nextJobId.incrementAndGet();

        // set connect string only if table is specified or EcsSync will create a db service with the default table
        SyncOptions options = syncConfig.getOptions();
        if (options.getDbTable() != null && options.getDbConnectString() == null && dbConnectString != null)
            options.setDbConnectString(dbConnectString);

        EcsSync sync = new EcsSync();
        sync.setSyncConfig(syncConfig);

        syncCache.put(jobId, sync);
        configCache.put(jobId, syncConfig);

        // start a background thread (otherwise this will block until the entire sync is done!)
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(new SyncTask(jobId, sync));
        executor.shutdown();

        return jobId;
    }

    public int registerJob(EcsSync sync) {
        int jobId = nextJobId.incrementAndGet();

        syncCache.put(jobId, sync);

        return jobId;
    }

    public SyncConfig getJob(int jobId) {
        if (syncCache.containsKey(jobId)) {
            if (!configCache.containsKey(jobId))
                throw new UnsupportedOperationException("specified job was created externally");

            return configCache.get(jobId);
        }
        return null;
    }

    public void deleteJob(int jobId) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) throw new IllegalArgumentException("the specified job ID does not exist");

        if (!getJobStatus(sync).isFinalState())
            throw new UnsupportedOperationException("the job must be stopped before it can be deleted");

        syncCache.remove(jobId);
        configCache.remove(jobId);

        // delete database
        if (sync.getDbService() != null) {
            sync.getDbService().deleteDatabase();
            try {
                sync.getDbService().close();
            } catch (IOException e) {
                log.warn("could not close database", e);
            }
        }
    }

    public JobControl getJobControl(int jobId) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) return null;

        JobControl jobControl = new JobControl();
        jobControl.setStatus(getJobStatus(sync));
        jobControl.setThreadCount(sync.getSyncConfig().getOptions().getThreadCount());

        return jobControl;
    }

    public void setJobControl(int jobId, JobControl jobControl) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) throw new IllegalArgumentException("the specified job ID does not exist");

        if (jobControl.getThreadCount() > 0) {
            sync.setThreadCount(jobControl.getThreadCount());
        }

        if (jobControl.getStatus() != null) {
            switch (jobControl.getStatus()) {
                case Stopped:
                    sync.terminate();
                    break;
                case Paused:
                    sync.pause();
                    break;
                case Running:
                    sync.resume();
                    break;
            }
        }
    }

    public SyncProgress getProgress(int jobId) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) return null;
        SyncStats stats = sync.getStats();

        SyncProgress syncProgress = new SyncProgress();
        syncProgress.setSyncStartTime(stats.getStartTime());
        syncProgress.setSyncStopTime(stats.getStopTime());
        syncProgress.setEstimatingTotals(sync.isEstimating());
        syncProgress.setTotalBytesExpected(sync.getEstimatedTotalBytes());
        syncProgress.setTotalObjectsExpected(sync.getEstimatedTotalObjects());
        syncProgress.setBytesComplete(stats.getBytesComplete());
        syncProgress.setObjectsComplete(stats.getObjectsComplete());
        syncProgress.setObjectsFailed(stats.getObjectsFailed());
        syncProgress.setActiveQueryTasks(sync.getActiveQueryThreads());
        syncProgress.setActiveSyncTasks(sync.getActiveSyncThreads());
        syncProgress.setRuntimeMs(stats.getTotalRunTime());
        syncProgress.setCpuTimeMs(stats.getTotalCpuTime());

        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        syncProgress.setProcessCpuLoad(osBean.getProcessCpuLoad());
        syncProgress.setProcessMemoryUsed(Runtime.getRuntime().totalMemory());

        // it's possible that the sync instance has not initialized its plugins yet
        if (sync.getSource() != null) {
            syncProgress.setSourceReadRate(sync.getSource().getReadRate());
            syncProgress.setSourceWriteRate(sync.getSource().getWriteRate());
        }
        if (sync.getTarget() != null) {
            syncProgress.setTargetReadRate(sync.getTarget().getReadRate());
            syncProgress.setTargetWriteRate(sync.getTarget().getWriteRate());
        }
        syncProgress.setObjectCompleteRate(sync.getStats().getObjectCompleteRate());
        syncProgress.setObjectErrorRate(sync.getStats().getObjectErrorRate());

        if (sync.getRunError() != null) syncProgress.setRunError(SyncUtil.summarize(sync.getRunError()));

        return syncProgress;
    }

    public Iterable<SyncRecord> getSyncErrors(int jobId) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) return null;

        if (sync.getDbService() == null) return Collections.emptyList();
        else return sync.getDbService().getSyncErrors();
    }

    protected JobControlStatus getJobStatus(EcsSync sync) {
        if (sync.isPaused()) return JobControlStatus.Paused;
        if (sync.isRunning()) return JobControlStatus.Running;
        if (sync.isTerminated()) {
            if (sync.getActiveSyncThreads() > 0) return JobControlStatus.Stopping;
            else return JobControlStatus.Stopped;
        }
        if (sync.getStats().getStopTime() > 0) return JobControlStatus.Complete;
        return JobControlStatus.Initialized;
    }

    public String getDbConnectString() {
        return dbConnectString;
    }

    /**
     * Sets the JDBC connect string for the mySQL database that will be used by all syncs specifying a dbTable
     */
    public void setDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
    }

    protected class SyncTask implements Runnable {
        private int jobId;
        private EcsSync sync;

        public SyncTask(int jobId, EcsSync sync) {
            this.jobId = jobId;
            this.sync = sync;
        }

        @Override
        public void run() {
            try {
                sync.run();
            } catch (Throwable t) {
                log.error("sync job " + jobId + " threw an unexpected error", t);
            }
        }
    }
}
