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
import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.rest.*;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.ConfigurationException;
import com.emc.ecs.sync.util.SyncUtil;
import com.emc.rest.smart.ecs.Vdc;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.propertyeditors.CustomDateEditor;

import java.beans.PropertyDescriptor;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncJobService {
    private static final Logger log = LoggerFactory.getLogger(SyncJobService.class);

    public static final String ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX";
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

        EcsSync sync = new EcsSync();

        configureSync(sync, syncConfig);

        syncCache.put(jobId, sync);
        configCache.put(jobId, syncConfig);

        // start a background thread (otherwise this will block until the entire sync is done!)
        new Thread(new SyncTask(jobId, sync)).start();

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
            sync.getDbService().close();
        }
    }

    public JobControl getJobControl(int jobId) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) return null;

        JobControl jobControl = new JobControl();
        jobControl.setStatus(getJobStatus(sync));
        jobControl.setSyncThreadCount(sync.getSyncThreadCount());
        jobControl.setQueryThreadCount(sync.getQueryThreadCount());

        return jobControl;
    }

    public void setJobControl(int jobId, JobControl jobControl) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) throw new IllegalArgumentException("the specified job ID does not exist");

        if (jobControl.getSyncThreadCount() > 0) {
            sync.setSyncThreadCount(jobControl.getSyncThreadCount());
        }

        if (jobControl.getQueryThreadCount() > 0) {
            sync.setQueryThreadCount(jobControl.getQueryThreadCount());
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

        SyncProgress syncProgress = new SyncProgress();
        syncProgress.setSyncStartTime(sync.getStartTime());
        syncProgress.setSyncStopTime(sync.getStopTime());
        syncProgress.setEstimatingTotals(sync.isEstimating());
        syncProgress.setTotalBytesExpected(sync.getEstimatedTotalBytes());
        syncProgress.setTotalObjectsExpected(sync.getEstimatedTotalObjects());
        syncProgress.setBytesComplete(sync.getBytesComplete());
        syncProgress.setObjectsComplete(sync.getObjectsComplete());
        syncProgress.setObjectsFailed(sync.getObjectsFailed());
        syncProgress.setActiveQueryTasks(sync.getActiveQueryThreads());
        syncProgress.setActiveSyncTasks(sync.getActiveSyncThreads());
        syncProgress.setRuntimeMs(sync.getTotalRunTime());
        syncProgress.setCpuTimeMs(sync.getTotalCpuTime());

        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        syncProgress.setProcessCpuLoad(osBean.getProcessCpuLoad());
        syncProgress.setProcessMemoryUsed(Runtime.getRuntime().totalMemory());

        syncProgress.setSourceReadRate(sync.getSource().getReadPerformance());
        syncProgress.setSourceWriteRate(sync.getSource().getWritePerformance());
        syncProgress.setTargetReadRate(sync.getTarget().getReadPerformance());
        syncProgress.setTargetWriteRate(sync.getTarget().getWritePerformance());
        syncProgress.setObjectCompleteRate(sync.getObjectCompleteRate());
        syncProgress.setObjectErrorRate(sync.getObjectErrorRate());

        if (sync.getRunError() != null) syncProgress.setRunError(SyncUtil.summarize(sync.getRunError()));

        return syncProgress;
    }

    public Iterable<SyncRecord> getSyncErrors(int jobId) {
        EcsSync sync = syncCache.get(jobId);

        if (sync == null) return null;

        if (sync.getDbService() == null) return Collections.emptyList();
        else return sync.getDbService().getSyncErrors();
    }

    /*
     Use Spring's handy BeanUtils to dynamically set standard and custom properties on plugins and sync instance.
     */
    protected void configureSync(EcsSync sync, SyncConfig syncConfig) {
        if (syncConfig.getSource() == null || syncConfig.getTarget() == null)
            throw new ConfigurationException("must specify source and target");

        sync.setSource(createPlugin(syncConfig.getSource(), syncConfig, SyncSource.class));
        sync.setTarget(createPlugin(syncConfig.getTarget(), syncConfig, SyncTarget.class));

        if (syncConfig.getFilters() != null) {
            for (PluginConfig filterConfig : syncConfig.getFilters()) {
                sync.getFilters().add(createPlugin(filterConfig, syncConfig, SyncFilter.class));
            }
        }

        copyProperties(syncConfig, sync, "source", "target", "filters"); // copy all except these properties

        // should not set connect string unless table is specified or EcsSync will create a db service with the default table
        if (sync.getDbTable() != null && sync.getDbConnectString() == null && dbConnectString != null)
            sync.setDbConnectString(dbConnectString);
    }

    @SuppressWarnings("unchecked")
    protected <T extends SyncPlugin> T createPlugin(PluginConfig pluginConfig, SyncConfig syncConfig, Class<T> clazz) {
        try {
            T plugin = (T) Class.forName(pluginConfig.getPluginClass()).newInstance();
            BeanWrapperImpl wrapper = new BeanWrapperImpl(plugin);

            // register property converters
            wrapper.registerCustomEditor(Date.class, new CustomDateEditor(new SimpleDateFormat(ISO_8601_FORMAT), true));
            wrapper.registerCustomEditor(Vdc.class, new VdcEditor());

            // set custom properties
            for (Map.Entry<String, String> prop : pluginConfig.getCustomProperties().entrySet()) {
                wrapper.setPropertyValue(prop.getKey(), prop.getValue());
            }
            // set custom list properties
            for (Map.Entry<String, List<String>> listProp : pluginConfig.getCustomListProperties().entrySet()) {
                wrapper.setPropertyValue(listProp.getKey(), listProp.getValue());
            }

            copyProperties(syncConfig, plugin); // set common properties

            return plugin;
        } catch (ClassCastException e) {
            throw new ConfigurationException(pluginConfig.getPluginClass() + " does not extend " + clazz.getSimpleName());
        } catch (BeansException e) {
            throw new ConfigurationException("could not set property on plugin " + pluginConfig.getPluginClass(), e);
        } catch (Throwable t) {
            throw new ConfigurationException("could not create plugin instance " + pluginConfig.getPluginClass(), t);
        }
    }

    protected void copyProperties(Object source, Object target, String... ignoredProperties) {
        List<String> ignoredList = Collections.emptyList();
        if (ignoredProperties != null) ignoredList = Arrays.asList(ignoredProperties);
        BeanWrapper wSource = new BeanWrapperImpl(source), wTarget = new BeanWrapperImpl(target);
        wSource.registerCustomEditor(Date.class, new CustomDateEditor(new SimpleDateFormat(ISO_8601_FORMAT), true));
        wTarget.registerCustomEditor(Date.class, new CustomDateEditor(new SimpleDateFormat(ISO_8601_FORMAT), true));
        for (PropertyDescriptor descriptor : wSource.getPropertyDescriptors()) {
            if (ignoredList.contains(descriptor.getName())) continue;
            if (!wSource.isReadableProperty(descriptor.getName())) continue;
            if (wTarget.isWritableProperty(descriptor.getName())) {

                // property is readable from source, writeable on target, and not in the list of ignored props
                Object value = wSource.getPropertyValue(descriptor.getName());
                if (value != null) wTarget.setPropertyValue(descriptor.getName(), value);
            }
        }
    }

    protected JobControlStatus getJobStatus(EcsSync sync) {
        if (sync.isPaused()) return JobControlStatus.Paused;
        if (sync.isRunning()) return JobControlStatus.Running;
        if (sync.isTerminated()) {
            if (sync.getActiveSyncThreads() > 0) return JobControlStatus.Stopping;
            else return JobControlStatus.Stopped;
        }
        if (sync.getStopTime() > 0) return JobControlStatus.Complete;
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
