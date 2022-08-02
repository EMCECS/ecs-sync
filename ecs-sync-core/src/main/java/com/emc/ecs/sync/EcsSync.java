/*
 * Copyright (c) 2014-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync;

import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.service.DbService;
import com.emc.ecs.sync.service.MySQLDbService;
import com.emc.ecs.sync.service.NoDbService;
import com.emc.ecs.sync.service.SqliteDbService;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.*;
import com.sun.management.OperatingSystemMXBean;
import engineering.clientside.throttle.Throttle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class EcsSync implements Runnable, RetryHandler, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(EcsSync.class);

    private DbService dbService;
    private Throwable runError;

    private EnhancedThreadPoolExecutor listExecutor;
    private EnhancedThreadPoolExecutor syncExecutor;
    private EnhancedThreadPoolExecutor queryExecutor;
    private EnhancedThreadPoolExecutor estimateQueryExecutor;
    private EnhancedThreadPoolExecutor estimateExecutor;
    private EnhancedThreadPoolExecutor retrySubmitter;
    private SyncFilter<?> firstFilter;
    private SyncEstimate syncEstimate;
    private volatile boolean terminated;
    private volatile boolean closed;
    private SyncStats stats = new SyncStats();

    private SyncConfig syncConfig;
    private SyncStorage<?> source;
    private SyncStorage<?> target;
    private List<SyncFilter<?>> filters;

    private SyncVerifier verifier;
    private final SyncControl syncControl = new SyncControl();

    private int perfReportSeconds;
    private ScheduledExecutorService perfScheduler;

    private final Set<OptionChangeListener> optionChangeListeners = new HashSet<>();
    private final Set<OperationListener> operationListeners = new HashSet<>();

    // TPS Throttle (objects/s)
    private Throttle throughputThrottle;
    // Bandwidth Throttle (bytes/s)
    private Throttle bandwidthThrottle;
    private Throttle sharedThroughputThrottle;
    private Throttle sharedBandwidthThrottle;

    public synchronized void run() {
        try {
            assert !closed : "this instance has been closed";

            assert syncConfig != null : "syncConfig is null";
            assert syncConfig.getOptions() != null : "syncConfig.options is null";
            final SyncOptions options = syncConfig.getOptions();

            // Some validation (must have source and target)
            assert source != null || syncConfig.getSource() != null : "source must be specified";
            assert target != null || syncConfig.getTarget() != null : "target plugin must be specified";

            // we are allowing users to either set plugin instances directly (with config objects already set),
            // or provide a config for each plugin in the syncConfig.
            // the order of priority for plugin instance and config will be:
            //     1) plugin instance set directly
            //     2) plugin generated from config
            // if a plugin instance is set directly, it must already have a config set, and that config object will
            // also be used in the syncConfig
            // NOTE: we also set the sync job on each plugin to this, so they all have access to the job context
            if (source == null) source = PluginUtil.newStorageFromConfig(syncConfig.getSource(), options);
            else syncConfig.setSource(source.getConfig());
            source.setSyncJob(this);

            if (target == null) target = PluginUtil.newStorageFromConfig(syncConfig.getTarget(), options);
            else syncConfig.setTarget(target.getConfig());
            target.setSyncJob(this);

            if (filters == null) {
                if (syncConfig.getFilters() != null)
                    filters = PluginUtil.newFiltersFromConfigList(syncConfig.getFilters(), options);
                else filters = new ArrayList<>();
            } else {
                List<Object> filterConfigs = new ArrayList<>();
                for (SyncFilter<?> filter : filters) {
                    filterConfigs.add(filter.getConfig());
                }
                syncConfig.setFilters(filterConfigs);
            }
            for (SyncFilter<?> filter : filters) {
                filter.setSyncJob(this);
            }

            if (options.getThroughputLimit() > 0) {
                throughputThrottle = Throttle.create(options.getThroughputLimit());
                log.info("applying job xput throttle: {}@{}",
                        throughputThrottle, Integer.toHexString(System.identityHashCode(throughputThrottle)));
            }
            if (options.getBandwidthLimit() > 0) {
                bandwidthThrottle = Throttle.create(options.getBandwidthLimit());
                log.info("applying job b/w throttle: {}@{}",
                        bandwidthThrottle, Integer.toHexString(System.identityHashCode(bandwidthThrottle)));
            }
            if (sharedThroughputThrottle != null) {
                log.info("applying shared xput throttle: {}@{}",
                        sharedThroughputThrottle, Integer.toHexString(System.identityHashCode(sharedThroughputThrottle)));
            }
            if (sharedBandwidthThrottle != null) {
                log.info("applying shared b/w throttle: {}@{}",
                        sharedBandwidthThrottle, Integer.toHexString(System.identityHashCode(sharedBandwidthThrottle)));
            }

            // Summarize config for reference
            if (log.isInfoEnabled()) log.info(summarizeConfig());

            // Ask each plugin to configure itself and validate the chain (resolves incompatible plugins)
            String currentPlugin = "source storage";
            try {
                source.configure(source, filters.iterator(), target);
                currentPlugin = "target storage";
                target.configure(source, filters.iterator(), target);
                for (SyncFilter<?> filter : filters) {
                    currentPlugin = filter.getClass().getSimpleName() + " filter";
                    filter.configure(source, filters.iterator(), target);
                }
            } catch (Exception e) {
                log.error("Error configuring " + currentPlugin);
                throw e;
            }

            // TODO: right now, plugins have no way to register themselves
            if (source instanceof OptionChangeListener) addOptionChangeListener((OptionChangeListener) source);
            if (target instanceof OptionChangeListener) addOptionChangeListener((OptionChangeListener) target);
            for (SyncFilter<?> filter : filters) {
                if (filter instanceof OptionChangeListener) addOptionChangeListener((OptionChangeListener) filter);
            }

            // Build the plugin chain
            Iterator<SyncFilter<?>> i = filters.iterator();
            SyncFilter<?> next, previous = null;
            while (i.hasNext()) {
                next = i.next();
                if (previous != null) previous.setNext(next);
                previous = next;
            }

            // add target to chain
            SyncFilter<?> targetFilter = new TargetFilter(target, options);
            if (previous != null) previous.setNext(targetFilter);

            firstFilter = filters.isEmpty() ? targetFilter : filters.get(0);

            // register for timings
            if (options.isTimingsEnabled()) TimingUtil.register(options);
            else TimingUtil.unregister(options); // in case of subsequent runs with same options instance

            log.info("Sync started at " + new Date());
            stats.setStartTime(System.currentTimeMillis());
            stats.setCpuStartTime(((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() / 1000000);

            // initialize DB Service if necessary
            if (dbService == null) {
                if (options.getDbFile() != null) {
                    dbService = new SqliteDbService(new File(options.getDbFile()), options.isDbEnhancedDetailsEnabled());
                } else if (options.getDbConnectString() != null) {
                    dbService = new MySQLDbService(options.getDbConnectString(), null, null, options.getDbEncPassword(),
                            options.isDbEnhancedDetailsEnabled());
                } else {
                    dbService = new NoDbService(options.isDbEnhancedDetailsEnabled());
                }
                if (options.getDbTable() != null) dbService.setObjectsTableName(options.getDbTable());
            }

            // create thread pools
            listExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<>(1000), "list-pool");
            estimateQueryExecutor = options.isEstimationEnabled()
                    ? new EnhancedThreadPoolExecutor(options.getThreadCount(), new LinkedBlockingDeque<>(), "estimate-q-pool")
                    : null;
            estimateExecutor = options.isEstimationEnabled()
                    ? new EnhancedThreadPoolExecutor(options.getThreadCount(), new LinkedBlockingDeque<>(1000), "estimate-pool")
                    : null;
            queryExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<>(), "query-pool");
            syncExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<>(1000), "sync-pool");
            retrySubmitter = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<>(), "retry-submitter");

            // initialize verifier
            verifier = new Md5Verifier(options);

            // setup performance reporting
            startPerformanceReporting();

            // set status to running
            syncControl.setRunning(true);
            stats.reset();
            log.info("syncing from {} to {}", ConfigUtil.generateUri(syncConfig.getSource(), true),
                    ConfigUtil.generateUri(syncConfig.getTarget(), true));

            if (options.isEstimationEnabled()) {
                // start estimating
                syncEstimate = new SyncEstimate();
                estimateExecutor.submit(() -> {
                    // do we have a raw list?
                    if (options.getSourceList() != null) {
                        for (String line : options.getSourceList()) {
                            estimateExecutor.blockingSubmit(new EstimateTask(line, source, syncEstimate));
                        }
                        // do we have a list file?
                    } else if (options.getSourceListFile() != null) {
                        LineIterator lineIterator = new LineIterator(options.getSourceListFile(),
                                options.isSourceListRawValues());
                        while (lineIterator.hasNext()) {
                            estimateExecutor.blockingSubmit(new EstimateTask(lineIterator.next(), source, syncEstimate));
                        }
                        // otherwise, enumerate the source storage
                    } else {
                        for (ObjectSummary summary : source.allObjects()) {
                            estimateExecutor.blockingSubmit(new EstimateTask(summary, source, syncEstimate));
                        }
                    }
                });
            }

            // iterate through root objects and submit tasks for syncing and crawling (querying).
            // raw list
            if (options.getSourceList() != null) {
                AtomicLong lineNum = new AtomicLong(0);
                for (String line : options.getSourceList()) {
                    lineNum.incrementAndGet();
                    if (!syncControl.isRunning()) break;
                    ObjectSummary summary = source.parseListLine(line);
                    summary.setListRowNum(lineNum.get()); // record the line number in the summary
                    submitForSync(source, summary);
                    if (options.isRecursive() && summary.isDirectory()) submitForQuery(source, summary);
                }
                // list file
            } else if (options.getSourceListFile() != null) { // do we have a list-file?
                LineIterator lineIterator = new LineIterator(options.getSourceListFile(),
                        options.isSourceListRawValues());
                AtomicLong lineNum = new AtomicLong(0);
                while (lineIterator.hasNext()) {
                    lineNum.incrementAndGet();
                    if (!syncControl.isRunning()) break;
                    String listLine = lineIterator.next();
                    listExecutor.blockingSubmit(() -> {
                        ObjectSummary summary = source.parseListLine(listLine);
                        summary.setListRowNum(lineNum.get()); // record the line number in the summary
                        submitForSync(source, summary);
                        if (options.isRecursive() && summary.isDirectory()) submitForQuery(source, summary);
                    });
                }
                // otherwise, enumerate the source
            } else {
                for (ObjectSummary summary : source.allObjects()) {
                    if (!syncControl.isRunning()) break;
                    submitForSync(source, summary);
                    if (options.isRecursive() && summary.isDirectory()) submitForQuery(source, summary);
                }
            }

            // now we must wait until all submitted tasks are complete
            while (syncControl.isRunning()) {
                if (listExecutor.getUnfinishedTasks() <= 0 && queryExecutor.getUnfinishedTasks() <= 0
                        && syncExecutor.getUnfinishedTasks() <= 0) {
                    // done
                    log.info("all tasks complete");
                    break;
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        log.warn("interrupted while sleeping", e);
                    }
                }
            }

            // run a final timing log
            TimingUtil.logTimings(options);
        } catch (Throwable t) {
            log.error("unexpected exception", t);
            runError = t;
            throw t;
        } finally {
            if (!syncControl.isRunning()) log.warn("terminated early!");
            syncControl.setRunning(false);
            if (listExecutor != null) listExecutor.shutdown();
            if (estimateQueryExecutor != null) estimateQueryExecutor.shutdown();
            if (estimateExecutor != null) estimateExecutor.shutdown();
            if (queryExecutor != null) queryExecutor.shutdown();
            if (retrySubmitter != null) retrySubmitter.shutdown();
            if (syncExecutor != null) {
                syncExecutor.shutdown();
                // if we were terminated early, wait for any in-progress sync tasks to finish
                try {
                    syncExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.warn("interrupted after termination while waiting for sync threads to finish", e);
                }
            }

            if (stats != null) stats.setStopTime(System.currentTimeMillis());

            // clean up any resources in the plugins
            close();
        }
    }

    private void startPerformanceReporting() {
        if (perfReportSeconds > 0) {
            perfScheduler = Executors.newSingleThreadScheduledExecutor();
            perfScheduler.scheduleAtFixedRate(
                    () -> {
                        if (isRunning()) {
                            log.info("Source: read: {} b/s write: {} b/s", getSource().getReadRate(),
                                    getSource().getWriteRate());
                            log.info("Target: read: {} b/s write: {} b/s", getTarget().getReadRate(),
                                    getTarget().getWriteRate());
                            log.info("Objects: complete: {}/s failed: {}/s", getStats().getObjectCompleteRate(),
                                    getStats().getObjectErrorRate());
                        }
                    },
                    perfReportSeconds, perfReportSeconds, TimeUnit.SECONDS);
        }
    }

    /**
     * Stops the underlying executors from executing new tasks. Currently running tasks will complete and all threads
     * will then block until resumed
     *
     * @throws IllegalStateException if the sync is complete or was terminated
     */
    public void pause() {
        if (!syncControl.isRunning()) throw new IllegalStateException("sync is not running");
        listExecutor.pause();
        if (estimateQueryExecutor != null) estimateQueryExecutor.pause();
        if (estimateExecutor != null) estimateExecutor.pause();
        queryExecutor.pause();
        retrySubmitter.pause();
        syncExecutor.pause();
        stats.pause();
    }

    /**
     * Resumes the underlying executors so they may continue to execute tasks
     *
     * @throws IllegalStateException if the sync is complete or was terminated
     * @see #pause()
     */
    public void resume() {
        if (!syncControl.isRunning()) throw new IllegalStateException("sync is not running");
        listExecutor.resume();
        if (estimateQueryExecutor != null) estimateQueryExecutor.resume();
        if (estimateExecutor != null) estimateExecutor.resume();
        queryExecutor.resume();
        retrySubmitter.resume();
        syncExecutor.resume();
        stats.resume();
    }

    public void terminate() {
        syncControl.setRunning(false);
        terminated = true;
        if (listExecutor != null) listExecutor.stop();
        if (estimateQueryExecutor != null) estimateQueryExecutor.stop();
        if (estimateExecutor != null) estimateExecutor.stop();
        if (queryExecutor != null) queryExecutor.stop();
        if (retrySubmitter != null) retrySubmitter.stop();
        if (syncExecutor != null) syncExecutor.stop();
    }

    public String summarizeConfig() {
        StringBuilder summary = new StringBuilder("Configuration Summary:\n");
        summary.append(ConfigUtil.summarize(syncConfig.getOptions()));
        summary.append("Source: ").append(ConfigUtil.summarize(syncConfig.getSource()));
        summary.append("Target: ").append(ConfigUtil.summarize(syncConfig.getTarget()));
        if (syncConfig.getFilters() != null) {
            summary.append("Filters:\n");
            for (Object filter : syncConfig.getFilters()) {
                summary.append(ConfigUtil.summarize(filter));
            }
        } else {
            summary.append("Filters: none\n");
        }
        return summary.toString();
    }

    private void submitForQuery(SyncStorage<?> source, ObjectSummary entry) {
        if (syncControl.isRunning()) queryExecutor.blockingSubmit(new QueryTask(source, entry));
        else log.debug("not submitting task for query because terminate() was called: " + entry.getIdentifier());
    }

    private void submitForSync(SyncStorage<?> source, ObjectContext objectContext) {
        // loop in case any throttle waits are interrupted
        while (true) {
            try {
                if (syncControl.isRunning()) {
                    // must honor both job-specific and shared throttling
                    // to eliminate overhead, we will acquire from both throttles asynchronously, and sleep for whichever delay is longer
                    long waitTime = 0, sharedWaitTime = 0;
                    if (throughputThrottle != null) waitTime = throughputThrottle.acquireDelayDuration(1);
                    if (sharedThroughputThrottle != null)
                        sharedWaitTime = sharedThroughputThrottle.acquireDelayDuration(1);
                    if (waitTime + sharedWaitTime > 0) NANOSECONDS.sleep(Math.max(waitTime, sharedWaitTime));

                    SyncTask syncTask = new SyncTask(objectContext, source, firstFilter, verifier,
                            dbService, this, syncControl, stats);
                    syncExecutor.blockingSubmit(syncTask);
                } else {
                    log.debug("not submitting task for sync because terminate() was called: " + objectContext.getSourceSummary().getIdentifier());
                }
                // only loop if we are interrupted
                break;
            } catch (InterruptedException e) {
                log.info("throttle wait was interrupted - this generally only happens if the job is terminated early", e);
            }
        }
    }

    private void submitForSync(SyncStorage<?> source, ObjectSummary summary) {
        ObjectContext objectContext = new ObjectContext();
        objectContext.setSourceSummary(summary);
        objectContext.setOptions(syncConfig.getOptions());
        objectContext.setStatus(ObjectStatus.Queue);
        submitForSync(source, objectContext);
    }

    @Override
    public void submitForRetry(final SyncStorage<?> source, final ObjectContext objectContext, Throwable t) throws Throwable {
        if (objectContext.getObject() == null || objectContext.getFailures() + 1 > syncConfig.getOptions().getRetryAttempts())
            throw t;
        objectContext.incFailures();

        // prepare for retry
        try {
            if (log.isInfoEnabled()) {
                log.info("O--R object " + objectContext.getSourceSummary().getIdentifier()
                        + " failed " + objectContext.getFailures() + " time" + (objectContext.getFailures() > 1 ? "s" : "")
                        + " (queuing for retry)", SyncUtil.getCause(t));
            }
            objectContext.setStatus(ObjectStatus.RetryQueue);
            dbService.setStatus(objectContext, SyncUtil.summarize(t), false);

            retrySubmitter.submit(() -> submitForSync(source, objectContext));
        } catch (Throwable t2) {
            // could not retry, so bubble original error
            log.warn("retry for {} failed: {}", objectContext.getSourceSummary().getIdentifier(), SyncUtil.getCause(t2));
            throw t;
        }
    }

    @Override
    public void close() {
        // make sure this instance cannot be run again
        closed = true;
        safeClose(stats);
        safeClose(source);
        if (filters != null) for (SyncFilter<?> filter : filters) {
            safeClose(filter);
        }
        safeClose(target);
        safeClose(verifier);
        if (perfScheduler != null) try {
            perfScheduler.shutdownNow();
        } catch (Throwable t) {
            log.warn("could not shut down perf reporting", t);
        }
    }

    private void safeClose(AutoCloseable closeable) {
        try {
            if (closeable != null) closeable.close();
        } catch (Throwable t) {
            log.warn("could not close " + closeable.getClass().getSimpleName(), t);
        }
    }

    public void setThreadCount(int threadCount) {
        syncConfig.getOptions().setThreadCount(threadCount);
        if (listExecutor != null) listExecutor.resizeThreadPool(threadCount);
        if (estimateQueryExecutor != null) estimateQueryExecutor.resizeThreadPool(threadCount);
        if (estimateExecutor != null) estimateExecutor.resizeThreadPool(threadCount);
        if (queryExecutor != null) queryExecutor.resizeThreadPool(threadCount);
        if (syncExecutor != null) syncExecutor.resizeThreadPool(threadCount);
        if (retrySubmitter != null) retrySubmitter.resizeThreadPool(threadCount);
        fireOptionsChangedEvent();
    }

    public DbService getDbService() {
        return dbService;
    }

    public void setDbService(DbService dbService) {
        this.dbService = dbService;
    }

    public Throwable getRunError() {
        return runError;
    }

    public SyncStats getStats() {
        return stats;
    }

    public boolean isRunning() {
        return syncControl.isRunning();
    }

    public boolean isPaused() {
        return syncExecutor != null && syncExecutor.isPaused();
    }

    public boolean isTerminated() {
        return terminated;
    }

    public boolean isEstimating() {
        return estimateExecutor != null && estimateExecutor.getUnfinishedTasks() > 0;
    }

    public long getEstimatedTotalObjects() {
        if (isEstimating() || syncEstimate == null) return -1;
        return syncEstimate.getTotalObjectCount();
    }

    public long getEstimatedTotalBytes() {
        if (isEstimating() || syncEstimate == null) return -1;
        return syncEstimate.getTotalByteCount();
    }

    public int getActiveQueryThreads() {
        if (queryExecutor != null) return queryExecutor.getActiveCount();
        return 0;
    }

    public int getActiveSyncThreads() {
        int count = 0;
        if (syncExecutor != null) count += syncExecutor.getActiveCount();
        return count;
    }

    /**
     * Counts the objects in the sync queue that have failed at least once (and are waiting to be retried)
     */
    public int getObjectsAwaitingRetry() {
        if (syncExecutor == null) return 0;
        int retryCount = 0;
        for (Runnable runnable : syncExecutor.getQueue().toArray(new Runnable[0])) {
            if (runnable instanceof EnhancedFutureTask) {
                EnhancedFutureTask<?> task = (EnhancedFutureTask<?>) runnable;
                SyncTask syncTask = (SyncTask) task.getRunnable();
                if (syncTask.getObjectContext().getStatus() == ObjectStatus.RetryQueue) retryCount++;
            }
        }
        return retryCount;
    }

    public void addOptionChangeListener(OptionChangeListener listener) {
        optionChangeListeners.add(listener);
    }

    public void removeOptionChangeListener(OptionChangeListener listener) {
        optionChangeListeners.remove(listener);
    }

    protected void fireOptionsChangedEvent() {
        for (OptionChangeListener listener : optionChangeListeners) {
            listener.optionsChanged(syncConfig.getOptions());
        }
    }

    public Set<OperationListener> getOperationListeners() {
        return operationListeners;
    }

    public void addOperationListener(OperationListener listener) {
        operationListeners.add(listener);
    }

    public void removeOperationListener(OperationListener listener) {
        operationListeners.remove(listener);
    }

    public SyncConfig getSyncConfig() {
        return syncConfig;
    }

    public void setSyncConfig(SyncConfig syncConfig) {
        this.syncConfig = syncConfig;
    }

    public int getPerfReportSeconds() {
        return perfReportSeconds;
    }

    public void setPerfReportSeconds(int perfReportSeconds) {
        this.perfReportSeconds = perfReportSeconds;
    }

    public SyncStorage<?> getSource() {
        return source;
    }

    public void setSource(SyncStorage<?> source) {
        this.source = source;
    }

    public SyncStorage<?> getTarget() {
        return target;
    }

    public void setTarget(SyncStorage<?> target) {
        this.target = target;
    }

    public List<SyncFilter<?>> getFilters() {
        return filters;
    }

    public void setFilters(List<SyncFilter<?>> filters) {
        this.filters = filters;
    }

    public Throttle getJobThroughputThrottle() {
        return throughputThrottle;
    }

    public Throttle getJobBandwidthThrottle() {
        return bandwidthThrottle;
    }

    public Throttle getSharedThroughputThrottle() {
        return sharedThroughputThrottle;
    }

    public void setSharedThroughputThrottle(Throttle sharedThroughputThrottle) {
        this.sharedThroughputThrottle = sharedThroughputThrottle;
    }

    public Throttle getSharedBandwidthThrottle() {
        return sharedBandwidthThrottle;
    }

    public void setSharedBandwidthThrottle(Throttle sharedBandwidthThrottle) {
        this.sharedBandwidthThrottle = sharedBandwidthThrottle;
    }

    private class QueryTask implements Runnable {
        private final SyncStorage<?> source;
        private final ObjectSummary parent;

        QueryTask(SyncStorage<?> source, ObjectSummary parent) {
            this.source = source;
            this.parent = parent;
        }

        @Override
        public void run() {
            if (!syncControl.isRunning()) {
                log.debug("aborting query task because terminate() was called: " + parent.getIdentifier());
                return;
            }
            try {
                if (parent.isDirectory()) {
                    log.debug(">>>> querying children of {}", parent.getIdentifier());
                    for (ObjectSummary child : source.children(parent)) {
                        submitForSync(source, child);

                        if (syncConfig.getOptions().isRecursive() && child.isDirectory()) {
                            log.debug("{} is directory; submitting for query", child);
                            submitForQuery(source, child);
                        }
                    }
                    log.debug("<<<< finished querying children of {}", parent.getIdentifier());
                }
            } catch (Throwable t) {
                log.warn(">>!! querying children of {} failed: {}", parent.getIdentifier(), SyncUtil.summarize(t));
                stats.incObjectsFailed();
                if (syncConfig.getOptions().isRememberFailed()) {
                    stats.addFailedObject(new FailedObject(parent.getListRowNum(), parent.getIdentifier()));
                }
            }
        }
    }

    private class EstimateTask implements Runnable {
        private String listLine;
        private ObjectSummary summary;
        private final SyncStorage<?> storage;
        private final SyncEstimate syncEstimate;

        EstimateTask(ObjectSummary summary, SyncStorage<?> storage, SyncEstimate syncEstimate) {
            this.summary = summary;
            this.storage = storage;
            this.syncEstimate = syncEstimate;
        }

        EstimateTask(String listLine, SyncStorage<?> storage, SyncEstimate syncEstimate) {
            this.listLine = listLine;
            this.storage = storage;
            this.syncEstimate = syncEstimate;
        }

        @Override
        public void run() {
            if (!syncControl.isRunning()) {
                log.debug("aborting estimate task because terminate() was called: " + summary.getIdentifier());
                return;
            }
            try {
                if (summary == null) summary = storage.parseListLine(listLine);
                syncEstimate.incTotalObjectCount(1);
                if (syncConfig.getOptions().isRecursive() && summary.isDirectory()) {
                    estimateQueryExecutor.blockingSubmit(() -> {
                        log.debug("[est.]>>>> querying children of {}", summary.getIdentifier());
                        for (ObjectSummary child : storage.children(summary)) {
                            estimateExecutor.blockingSubmit(new EstimateTask(child, storage, syncEstimate));
                        }
                        log.debug("[est.]<<<< finished querying children of {}", summary.getIdentifier());
                    });
                } else {
                    syncEstimate.incTotalByteCount(summary.getSize());
                }
            } catch (Throwable t) {
                log.warn("unexpected exception", t);
            }
        }
    }
}
