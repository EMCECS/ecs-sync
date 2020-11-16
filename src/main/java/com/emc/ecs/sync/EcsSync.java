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
package com.emc.ecs.sync;

import com.emc.ecs.sync.cli.CliConfig;
import com.emc.ecs.sync.cli.CliHelper;
import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.ConfigWrapper;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.SyncOptions;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.*;
import com.emc.ecs.sync.rest.RestServer;
import com.emc.ecs.sync.service.*;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.*;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EcsSync implements Runnable, RetryHandler {
    private static final Logger log = LoggerFactory.getLogger(EcsSync.class);

    public static final String VERSION = EcsSync.class.getPackage().getImplementationVersion();

    public static void main(String[] args) {
        int exitCode = 0;

        System.out.println(versionLine());

        RestServer restServer = null;
        try {

            // first, hush up the JDK logger (why does this default to INFO??)
            java.util.logging.LogManager.getLogManager().getLogger("").setLevel(java.util.logging.Level.WARNING);

            CliConfig cliConfig = CliHelper.parseCliConfig(args);

            if (cliConfig != null) {

                // configure logging for startup
                if (cliConfig.getLogLevel() != null)
                    SyncJobService.getInstance().setLogLevel(cliConfig.getLogLevel());

                // start REST service
                if (cliConfig.isRestEnabled()) {
                    if (cliConfig.getRestEndpoint() != null) {
                        String[] endpoint = cliConfig.getRestEndpoint().split(":");
                        restServer = new RestServer(endpoint[0], Integer.parseInt(endpoint[1]));
                    } else {
                        restServer = new RestServer();
                        restServer.setAutoPortEnabled(true);
                    }
                    // set DB connect string if provided
                    if (cliConfig.getDbConnectString() != null) {
                        SyncJobService.getInstance().setDbConnectString(cliConfig.getDbConnectString());
                    }
                    // set encrypted DB password if provided
                    if (cliConfig.getDbEncPassword() != null) {
                        SyncJobService.getInstance().setDbEncPassword(cliConfig.getDbEncPassword());
                    }
                    restServer.start();
                }

                // if REST-only, skip remaining logic (REST server thread will keep the VM running)
                if (cliConfig.isRestOnly()) return;

                try {
                    // determine sync config
                    SyncConfig syncConfig;
                    if (cliConfig.getXmlConfig() != null) {
                        syncConfig = loadXmlFile(new File(cliConfig.getXmlConfig()));
                    } else {
                        syncConfig = CliHelper.parseSyncConfig(cliConfig, args);
                    }

                    // create the sync instance
                    final EcsSync sync = new EcsSync();
                    sync.setSyncConfig(syncConfig);

                    // register for REST access
                    SyncJobService.getInstance().registerJob(sync);

                    // start sync job (this blocks until the sync is complete)
                    sync.run();

                    // print completion stats
                    System.out.print(sync.getStats().getStatsString());
                    if (sync.getStats().getObjectsFailed() > 0) exitCode = 3;
                } finally {
                    if (restServer != null) try {
                        restServer.stop(0);
                    } catch (Throwable t) {
                        log.warn("could not stop REST service", t);
                    }
                }
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.out.println("    use --help for a detailed (quite long) list of options");
            exitCode = 1;
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 2;
        }

        // 0 = completed with no failures, 1 = invalid options, 2 = unexpected error, 3 = completed with some object failures
        System.exit(exitCode);
    }

    private static SyncConfig loadXmlFile(File xmlFile) throws JAXBException {
        List<Class> pluginClasses = new ArrayList<>();
        pluginClasses.add(SyncConfig.class);
        for (ConfigWrapper<?> wrapper : ConfigUtil.allStorageConfigWrappers()) {
            pluginClasses.add(wrapper.getTargetClass());
        }
        for (ConfigWrapper<?> wrapper : ConfigUtil.allFilterConfigWrappers()) {
            pluginClasses.add(wrapper.getTargetClass());
        }
        return (SyncConfig) JAXBContext.newInstance(pluginClasses.toArray(new Class[pluginClasses.size()]))
                .createUnmarshaller().unmarshal(xmlFile);
    }

    private static String versionLine() {
        return EcsSync.class.getSimpleName() + (VERSION == null ? "" : " v" + VERSION);
    }

    private DbService dbService;
    private Throwable runError;

    private EnhancedThreadPoolExecutor listExecutor;
    private EnhancedThreadPoolExecutor syncExecutor;
    private EnhancedThreadPoolExecutor queryExecutor;
    private EnhancedThreadPoolExecutor estimateQueryExecutor;
    private EnhancedThreadPoolExecutor estimateExecutor;
    private EnhancedThreadPoolExecutor retrySubmitter;
    private SyncFilter firstFilter;
    private SyncEstimate syncEstimate;
    private volatile boolean terminated;
    private SyncStats stats = new SyncStats();

    private SyncConfig syncConfig;
    private SyncStorage<?> source;
    private SyncStorage<?> target;
    private List<SyncFilter> filters;

    private SyncVerifier verifier;
    private SyncControl syncControl = new SyncControl();

    private int perfReportSeconds;
    private ScheduledExecutorService perfScheduler;

    private Set<OptionChangeListener> optionChangeListeners = new HashSet<>();

    public void run() {
        try {
            assert syncConfig != null : "syncConfig is null";
            assert syncConfig.getOptions() != null : "syncConfig.options is null";
            final SyncOptions options = syncConfig.getOptions();

            // Some validation (must have source and target)
            assert source != null || syncConfig.getSource() != null : "source must be specified";
            assert target != null || syncConfig.getTarget() != null : "target plugin must be specified";

            if (source == null) source = PluginUtil.newStorageFromConfig(syncConfig.getSource(), options);
            else syncConfig.setSource(source.getConfig());

            if (target == null) target = PluginUtil.newStorageFromConfig(syncConfig.getTarget(), options);
            else syncConfig.setTarget(target.getConfig());

            if (filters == null) {
                if (syncConfig.getFilters() != null)
                    filters = PluginUtil.newFiltersFromConfigList(syncConfig.getFilters(), options);
                else filters = new ArrayList<>();
            } else {
                List<Object> filterConfigs = new ArrayList<>();
                for (SyncFilter filter : filters) {
                    filterConfigs.add(filter.getConfig());
                }
                syncConfig.setFilters(filterConfigs);
            }

            // Summarize config for reference
            if (log.isInfoEnabled()) log.info(summarizeConfig());

            // Ask each plugin to configure itself and validate the chain (resolves incompatible plugins)
            String currentPlugin = "source storage";
            try {
                source.configure(source, filters.iterator(), target);
                currentPlugin = "target storage";
                target.configure(source, filters.iterator(), target);
                for (SyncFilter filter : filters) {
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
            for (SyncFilter filter : filters) {
                if (filter instanceof OptionChangeListener) addOptionChangeListener((OptionChangeListener) filter);
            }

            // Build the plugin chain
            Iterator<SyncFilter> i = filters.iterator();
            SyncFilter next, previous = null;
            while (i.hasNext()) {
                next = i.next();
                if (previous != null) previous.setNext(next);
                previous = next;
            }

            // add target to chain
            SyncFilter targetFilter = new TargetFilter(target, options);
            if (previous != null) previous.setNext(targetFilter);

            firstFilter = filters.isEmpty() ? targetFilter : filters.get(0);

            // register for timings
            if (options.isTimingsEnabled()) TimingUtil.register(options);
            else TimingUtil.unregister(options); // in case of subsequent runs with same options instance

            log.info("Sync started at " + new Date());
            // make sure any old stats are closed to terminate the counter threads
            try (SyncStats oldStats = stats) {
                stats = new SyncStats();
            }
            stats.setStartTime(System.currentTimeMillis());
            stats.setCpuStartTime(((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() / 1000000);

            // initialize DB Service if necessary
            if (dbService == null) {
                if (options.getDbFile() != null) {
                    dbService = new SqliteDbService(options.getDbFile());
                } else if (options.getDbConnectString() != null) {
                    dbService = new MySQLDbService(options.getDbConnectString(), null, null, options.getDbEncPassword());
                } else {
                    dbService = new NoDbService();
                }
                if (options.getDbTable() != null) dbService.setObjectsTableName(options.getDbTable());
            }

            // create thread pools
            listExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<Runnable>(1000), "list-pool");
            estimateQueryExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<Runnable>(), "estimate-q-pool");
            estimateExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<Runnable>(1000), "estimate-pool");
            queryExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<Runnable>(), "query-pool");
            syncExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<Runnable>(1000), "sync-pool");
            retrySubmitter = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                    new LinkedBlockingDeque<Runnable>(), "retry-submitter");

            // initialize verifier
            verifier = new Md5Verifier(options);

            // setup performance reporting
            startPerformanceReporting();

            // set status to running
            syncControl.setRunning(true);
            stats.reset();
            log.info("syncing from {} to {}", ConfigUtil.generateUri(syncConfig.getSource()),
                    ConfigUtil.generateUri(syncConfig.getTarget()));

            // start estimating
            syncEstimate = new SyncEstimate();
            estimateExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    // do we have a list-file?
                    if (options.getSourceListFile() != null) {
                        LineIterator lineIterator = new LineIterator(options.getSourceListFile(),
                                options.isSourceListFileRawValues());
                        while (lineIterator.hasNext()) {
                            estimateExecutor.blockingSubmit(new EstimateTask(lineIterator.next(), source, syncEstimate));
                        }
                    } else if (options.isEstimationEnabled()) {
                        for (ObjectSummary summary : source.allObjects()) {
                            estimateExecutor.blockingSubmit(new EstimateTask(summary, source, syncEstimate));
                        }
                    }
                }
            });

            // iterate through root objects and submit tasks for syncing and crawling (querying).
            if (options.getSourceListFile() != null) { // do we have a list-file?
                LineIterator lineIterator = new LineIterator(options.getSourceListFile(),
                        options.isSourceListFileRawValues());
                while (lineIterator.hasNext()) {
                    if (!syncControl.isRunning()) break;
                    final String listLine = lineIterator.next();
                    listExecutor.blockingSubmit(new Runnable() {
                        @Override
                        public void run() {
                            ObjectSummary summary = source.parseListLine(listLine);
                            submitForSync(source, summary);
                            if (options.isRecursive() && summary.isDirectory()) submitForQuery(source, summary);
                        }
                    });
                }
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
            if (syncExecutor != null) syncExecutor.shutdown();
            if (stats != null) stats.setStopTime(System.currentTimeMillis());

            // clean up any resources in the plugins
            cleanup();
        }
    }

    private void startPerformanceReporting() {
        if (perfReportSeconds > 0) {
            perfScheduler = Executors.newSingleThreadScheduledExecutor();
            perfScheduler.scheduleAtFixedRate(
                    new Runnable() {
                        @Override
                        public void run() {
                            if (isRunning()) {
                                log.info("Source: read: {} b/s write: {} b/s", getSource().getReadRate(),
                                        getSource().getWriteRate());
                                log.info("Target: read: {} b/s write: {} b/s", getTarget().getReadRate(),
                                        getTarget().getWriteRate());
                                log.info("Objects: complete: {}/s failed: {}/s", getStats().getObjectCompleteRate(),
                                        getStats().getObjectErrorRate());
                            }
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
        estimateQueryExecutor.pause();
        estimateExecutor.pause();
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
        estimateQueryExecutor.resume();
        estimateExecutor.resume();
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

    private void submitForQuery(SyncStorage source, ObjectSummary entry) {
        if (syncControl.isRunning()) queryExecutor.blockingSubmit(new QueryTask(source, entry));
        else log.debug("not submitting task for query because terminate() was called: " + entry.getIdentifier());
    }

    private void submitForSync(SyncStorage source, ObjectContext objectContext) {
        if (syncControl.isRunning()) {
            SyncTask syncTask = new SyncTask(objectContext, source, firstFilter, verifier,
                    dbService, this, syncControl, stats);
            syncExecutor.blockingSubmit(syncTask);
        } else {
            log.debug("not submitting task for sync because terminate() was called: " + objectContext.getSourceSummary().getIdentifier());
        }
    }

    private void submitForSync(SyncStorage source, ObjectSummary summary) {
        ObjectContext objectContext = new ObjectContext();
        objectContext.setSourceSummary(summary);
        objectContext.setOptions(syncConfig.getOptions());
        objectContext.setStatus(ObjectStatus.Queue);
        submitForSync(source, objectContext);
    }

    @Override
    public void submitForRetry(final SyncStorage source, final ObjectContext objectContext, Throwable t) throws Throwable {
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

            retrySubmitter.submit(new Runnable() {
                @Override
                public void run() {
                    submitForSync(source, objectContext);
                }
            });
        } catch (Throwable t2) {
            // could not retry, so bubble original error
            log.warn("retry for {} failed: {}", objectContext.getSourceSummary().getIdentifier(), SyncUtil.getCause(t2));
            throw t;
        }
    }

    protected void cleanup() {
        safeClose(stats);
        safeClose(source);
        if (filters != null) for (SyncFilter filter : filters) {
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

    public List<SyncFilter> getFilters() {
        return filters;
    }

    public void setFilters(List<SyncFilter> filters) {
        this.filters = filters;
    }

    private class QueryTask implements Runnable {
        private SyncStorage<?> source;
        private ObjectSummary parent;

        QueryTask(SyncStorage source, ObjectSummary parent) {
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
                if (syncConfig.getOptions().isRememberFailed()) stats.addFailedObject(parent.getIdentifier());
            }
        }
    }

    private class EstimateTask implements Runnable {
        private String listLine;
        private ObjectSummary summary;
        private SyncStorage<?> storage;
        private SyncEstimate syncEstimate;

        EstimateTask(ObjectSummary summary, SyncStorage storage, SyncEstimate syncEstimate) {
            this.summary = summary;
            this.storage = storage;
            this.syncEstimate = syncEstimate;
        }

        EstimateTask(String listLine, SyncStorage storage, SyncEstimate syncEstimate) {
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
                if (syncConfig.getOptions().isEstimationEnabled() && syncConfig.getOptions().isRecursive() && summary.isDirectory()) {
                    estimateQueryExecutor.blockingSubmit(new Runnable() {
                        @Override
                        public void run() {
                            log.debug("[est.]>>>> querying children of {}", summary.getIdentifier());
                            for (ObjectSummary child : storage.children(summary)) {
                                estimateExecutor.blockingSubmit(new EstimateTask(child, storage, syncEstimate));
                            }
                            log.debug("[est.]<<<< finished querying children of {}", summary.getIdentifier());
                        }
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
