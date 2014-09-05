/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync;

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import com.emc.vipr.sync.util.TimingUtil;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogMF;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.util.Assert;

import java.io.File;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * New plugin-based sync program.  Can be configured in two ways:
 * 1) through a command-line parser
 * 2) through Spring.  Call run() on the ViPRSync object after your beans are
 * initialized.
 *
 * @author cwikj
 */
public class ViPRSync implements Runnable {
    private static final Logger l4j = Logger.getLogger(ViPRSync.class);

    private static final String HELP_OPTION = "help";
    private static final String HELP_DESC = "Displays this help content";

    private static final String ROOT_SPRING_BEAN = "sync";
    private static final String SPRING_CONFIG_OPTION = "spring-config";
    private static final String SPRING_CONFIG_DESC = "Specifies a Spring bean configuration file. In this mode, Spring is used to initialize the application configuration from a spring context XML file.  It is assumed that there is a bean named '" + ROOT_SPRING_BEAN + "' containing an ViPRSync object.  This object will be initialized and executed.  In this mode all other CLI arguments are ignored.";
    private static final String SPRING_CONFIG_ARG_NAME = "path-to-spring-file";

    private static final String SOURCE_OPTION = "source";
    private static final String SOURCE_DESC = "The URI for the synchronization source.  Examples:\n" +
            "atmos:http://uid:secret@host:port  -- Uses Atmos as the source; could also be https.\n" +
            "file:///tmp/atmos/           -- Reads from a directory\n" +
            "archive:///tmp/atmos/backup.tar.gz  -- Reads from an archive file\n" +
            "s3:bucket-name               -- Reads from an S3 bucket\n" +
            "\nOther plugins may be available.  See their documentation for URI formats";
    private static final String SOURCE_ARG_NAME = "source-uri";

    private static final String TARGET_OPTION = "target";
    private static final String TARGET_DESC = "The URI for the synchronization target.  Examples:\n" +
            "atmos:http://uid:secret@host:port  -- Uses Atmos as the target; could also be https.\n" +
            "file:///tmp/atmos/           -- Writes to a directory\n" +
            "archive:///tmp/atmos/backup.tar.gz  -- Writes to an archive file\n" +
            "s3:bucket-name               -- Writes to an S3 bucket\n" +
            "\nOther plugins may be available.  See their documentation for URI formats";
    private static final String TARGET_ARG_NAME = "target-uri";

    private static final String FILTERS_OPTION = "filters";
    private static final String FILTERS_DESC = "The comma-delimited list of filters to apply to objects as they are synced. " +
            "Specify the activation names of the filters [returned from Filter.getActivationName()]. Examples:\n" +
            "    id-logging,retry\n" +
            "    db-id-mapping,strip-acls,retry\n" +
            "Each filter may have additional custom parameters you may specify separately";
    private static final String FILTERS_ARG_NAME = "filter-names";

    private static final String QUERY_THREADS_OPTION = "query-threads";
    private static final String QUERY_THREADS_DESC = "Specifies the number of threads to use when querying for child objects";
    private static final String QUERY_THREADS_ARG_NAME = "thread-count";

    private static final String SYNC_THREADS_OPTION = "sync-threads";
    private static final String SYNC_THREADS_DESC = "Specifies the number of threads to use when syncing objects";
    private static final String SYNC_THREADS_ARG_NAME = "thread-count";

    private static final String TIMINGS_OPTION = "timing";
    private static final String TIMINGS_DESC = "Enabled timings for all plug-ins that support it.  When specified, plug-ins will collect and periodically log average timing for various operations (i.e. read-metadata, stream-object, write-metadata, etc.)";

    private static final String TIMING_WINDOW_OPTION = "timing-window";
    private static final String TIMING_WINDOW_DESC = "Sets the timing window to use for timings.  Every {window-size} objects, timing statistics will be averaged and logged";
    private static final String TIMING_WINDOW_ARG_NAME = "window-size";

    private static final String NON_RECURSIVE_OPTION = "non-recursive";
    private static final String NON_RECURSIVE_DESC = "By default, hierarchical sources will be synced recursively (i.e. Atmos source w/ namespace and filesystem source). Use this option to disable recursive behavior and sync only at the given level.";

    private static final String FORGET_FAILED_OPTION = "forget-failed";
    private static final String FORGET_FAILED_DESC = "By default, ViPRSync tracks all failed objects and displays a summary of failures when finished. To save memory in large migrations, this option will disable this summary. If you use this option, be sure your logging is at an appropriate level and that you are capturing failures in a log file.";

    private static final String DELETE_SOURCE_OPTION = "delete-source";
    private static final String DELETE_SOURCE_DESC = "Supported source plugins will delete each source object once it is successfully synced (does not include directories). Use this option with care! Be sure log levels are appropriate to capture transferred (source deleted) objects.";

    // logging options
    private static final String DEBUG_OPTION = "debug";
    private static final String DEBUG_DESC = "Sets log threshold to DEBUG";
    private static final String VERBOSE_OPTION = "verbose";
    private static final String VERBOSE_DESC = "Sets log threshold to INFO";
    private static final String QUIET_OPTION = "quiet";
    private static final String QUIET_DESC = "Sets log threshold to WARNING";
    private static final String SILENT_OPTION = "silent";
    private static final String SILENT_DESC = "Disables logging";

    private static ServiceLoader<SyncSource> sourceLoader = ServiceLoader.load(SyncSource.class);
    private static ServiceLoader<SyncFilter> filterLoader = ServiceLoader.load(SyncFilter.class);
    private static ServiceLoader<SyncTarget> targetLoader = ServiceLoader.load(SyncTarget.class);

    private static GnuParser gnuParser = new GnuParser();

    public static void main(String[] args) {
        ViPRSync sync;
        try {
            CommandLine line = gnuParser.parse(mainOptions(), args);

            // Special check for help
            if (line.hasOption(HELP_OPTION)) {
                longHelp();
                System.exit(0);
            }

            if (line.hasOption(SPRING_CONFIG_OPTION)) {

                // Spring configuration
                sync = springBootstrap(line.getOptionValue(SPRING_CONFIG_OPTION));
            } else {

                // CLI configuration
                sync = cliBootstrap(args);
            }
        } catch (ParseException | ConfigurationException e) {
            System.err.println(e.getMessage());
            shortHelp();
            System.exit(1);
            return;
        }

        int exitCode = 0;
        try {
            sync.run();
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 2;
        }

        System.out.print(sync.getStatsString());

        System.exit(exitCode);
    }

    /**
     * Initializes a Spring Application Context from the given file and
     * bootstraps the ViPRSync object from there.
     */
    protected static ViPRSync springBootstrap(String pathToSpringXml) {
        File springXml = new File(pathToSpringXml);
        if (!springXml.exists()) {
            throw new ConfigurationException("the Spring XML file: " + springXml + " does not exist");
        }

        l4j.info("loading configuration from Spring XML file: " + springXml);
        FileSystemXmlApplicationContext ctx =
                new FileSystemXmlApplicationContext(pathToSpringXml);

        if (!ctx.containsBean(ROOT_SPRING_BEAN)) {
            throw new ConfigurationException("your Spring XML file: " + springXml + " must contain one bean named '" +
                    ROOT_SPRING_BEAN + "' that initializes an ViPRSync object");
        }

        return ctx.getBean(ROOT_SPRING_BEAN, ViPRSync.class);
    }

    /**
     * Loads and configures plugins based on command line options.
     */
    protected static ViPRSync cliBootstrap(String[] args) throws ParseException {
        ViPRSync sync = new ViPRSync();
        List<SyncPlugin> plugins = new ArrayList<>();

        CommandLine line = gnuParser.parse(mainOptions(), args, true);

        // find a plugin that can read from the source
        String sourceUri = line.getOptionValue(SOURCE_OPTION);
        if (sourceUri != null) {
            for (SyncSource source : sourceLoader) {
                if (source.canHandleSource(sourceUri)) {
                    source.setSourceUri(sourceUri);
                    sync.setSource(source);
                    plugins.add(source);
                    LogMF.info(l4j, "source: {0} ({1})", source.getName(), source.getClass());
                    break;
                }
            }
        }

        String targetUri = line.getOptionValue(TARGET_OPTION);
        // find a plugin that can write to the target
        if (targetUri != null) {
            for (SyncTarget target : targetLoader) {
                if (target.canHandleTarget(targetUri)) {
                    target.setTargetUri(targetUri);
                    sync.setTarget(target);
                    plugins.add(target);
                    LogMF.info(l4j, "target: {0} ({1})", target.getName(), target.getClass());
                    break;
                }
            }
        }

        // load filters
        List<SyncFilter> filters = new ArrayList<>();
        String filtersParameter = line.getOptionValue(FILTERS_OPTION);
        if (filtersParameter != null) {
            for (String filterName : filtersParameter.split(",")) {
                for (SyncFilter filter : filterLoader) {
                    if (filter.getActivationName().equals(filterName)) {
                        filters.add(filter);
                        plugins.add(filter);
                        LogMF.info(l4j, "filter: {0} ({1})", filter.getName(), filter.getClass());
                        break;
                    }
                }
            }
            sync.setFilters(filters);
        }

        // configure thread counts
        if (line.hasOption(QUERY_THREADS_OPTION))
            sync.setQueryThreadCount(Integer.parseInt(line.getOptionValue(QUERY_THREADS_OPTION)));
        if (line.hasOption(SYNC_THREADS_OPTION))
            sync.setSyncThreadCount(Integer.parseInt(line.getOptionValue(SYNC_THREADS_OPTION)));

        // configure timings display
        if (line.hasOption(TIMINGS_OPTION)) sync.setTimingsEnabled(true);
        if (line.hasOption(TIMING_WINDOW_OPTION)) {
            sync.setTimingWindow(Integer.parseInt(line.getOptionValue(TIMING_WINDOW_OPTION)));
        }

        // configure recursive behavior
        if (line.hasOption(NON_RECURSIVE_OPTION)) sync.setRecursive(false);

        // configure failed object tracking
        if (line.hasOption(FORGET_FAILED_OPTION)) sync.setRememberFailed(false);

        // configure whether to delete source objects after they are successfully synced
        if (line.hasOption(DELETE_SOURCE_OPTION)) sync.setDeleteSource(true);

        // logging options
        if (line.hasOption(DEBUG_OPTION)) {
            sync.setLogLevel(DEBUG_OPTION);
        }
        if (line.hasOption(VERBOSE_OPTION)) {
            sync.setLogLevel(VERBOSE_OPTION);
        }
        if (line.hasOption(QUIET_OPTION)) {
            sync.setLogLevel(QUIET_OPTION);
        }
        if (line.hasOption(SILENT_OPTION)) {
            sync.setLogLevel(SILENT_OPTION);
        }

        // Quick check for no-args
        if (sync.getSource() == null) {
            throw new ConfigurationException("source must be specified");
        }
        if (sync.getTarget() == null) {
            throw new ConfigurationException("target must be specified");
        }

        // Let the plugins parse their own options
        //   1. add common options and all the options from the plugins
        Options options = mainOptions();
        for (Object o : CommonOptions.getOptions().getOptions()) {
            options.addOption((Option) o);
        }
        for (SyncPlugin plugin : plugins) {
            for (Object o : plugin.getCustomOptions().getOptions()) {
                Option option = (Option) o;
                if (options.hasOption(option.getOpt())) {
                    System.err.println("WARNING: The option " + option.getOpt() +
                            " is being used by more than one plugin");
                }
                options.addOption(option);
            }
        }
        //   2. re-parse the command line based on these options
        line = gnuParser.parse(options, args);
        if (l4j.isDebugEnabled()) {
            for (Option option : line.getOptions()) {
                if (option.hasArg())
                    LogMF.debug(l4j, "parsed option {0}: {1}", option.getLongOpt(), line.getOptionValue(option.getLongOpt()));
                else
                    LogMF.debug(l4j, "parsed option {0}", option.getLongOpt());
            }
        }
        //   3. pass the result to each plugin separately
        for (SyncPlugin plugin : plugins) {
            plugin.parseOptions(line);
        }

        return sync;
    }

    protected static void shortHelp() {
        System.out.println("    use --help for a detailed (quite long) list of options");
    }

    protected static void longHelp() {
        HelpFormatter fmt = new HelpFormatter();

        Options options = mainOptions();
        for (Object o : CommonOptions.getOptions().getOptions()) {
            options.addOption((Option) o);
        }

        // Make sure we do CommonOptions first
        fmt.printHelp("java -jar vipr-sync.jar -source <source-uri> [-filters <filter1>[,<filter2>,...]] -target <target-uri> [options]\n" +
                "Common Options:", options);

        System.out.println("\nThe following plugins are also available and can be configured with their own options:\n");

        // Do the rest
        fmt.setSyntaxPrefix("");
        for (SyncSource source : sourceLoader) {
            String summary = String.format("%s (%s)\n%s", source.getName(), source.getClass().getName(), source.getDocumentation());
            fmt.printHelp(summary, source.getCustomOptions());
        }
        for (SyncTarget target : targetLoader) {
            String summary = String.format("%s (%s)\n%s", target.getName(), target.getClass().getName(), target.getDocumentation());
            fmt.printHelp(summary, target.getCustomOptions());
        }
        for (SyncFilter filter : filterLoader) {
            String summary = String.format("%s (%s), activated by: %s\n%s", filter.getName(), filter.getClass().getName(), filter.getActivationName(), filter.getDocumentation());
            fmt.printHelp(summary, filter.getCustomOptions());
        }
    }

    protected static Options mainOptions() {
        Options options = new Options();
        options.addOption(new OptionBuilder().withLongOpt(HELP_OPTION).withDescription(HELP_DESC).create());
        options.addOption(new OptionBuilder().withLongOpt(SPRING_CONFIG_OPTION).withDescription(SPRING_CONFIG_DESC)
                .hasArg().withArgName(SPRING_CONFIG_ARG_NAME).create());
        options.addOption(new OptionBuilder().withLongOpt(QUERY_THREADS_OPTION).withDescription(QUERY_THREADS_DESC)
                .hasArg().withArgName(QUERY_THREADS_ARG_NAME).create());
        options.addOption(new OptionBuilder().withLongOpt(SYNC_THREADS_OPTION).withDescription(SYNC_THREADS_DESC)
                .hasArg().withArgName(SYNC_THREADS_ARG_NAME).create());
        options.addOption(new OptionBuilder().withLongOpt(SOURCE_OPTION).withDescription(SOURCE_DESC)
                .hasArg().withArgName(SOURCE_ARG_NAME).create());
        options.addOption(new OptionBuilder().withLongOpt(TARGET_OPTION).withDescription(TARGET_DESC)
                .hasArg().withArgName(TARGET_ARG_NAME).create());
        options.addOption(new OptionBuilder().withLongOpt(FILTERS_OPTION).withDescription(FILTERS_DESC)
                .hasArg().withArgName(FILTERS_ARG_NAME).create());
        options.addOption(new OptionBuilder().withLongOpt(NON_RECURSIVE_OPTION).withDescription(NON_RECURSIVE_DESC).create());
        options.addOption(new OptionBuilder().withLongOpt(TIMINGS_OPTION).withDescription(TIMINGS_DESC).create());
        options.addOption(new OptionBuilder().withLongOpt(TIMING_WINDOW_OPTION).withDescription(TIMING_WINDOW_DESC)
                .hasArg().withArgName(TIMING_WINDOW_ARG_NAME).create());
        options.addOption(new OptionBuilder().withLongOpt(FORGET_FAILED_OPTION).withDescription(FORGET_FAILED_DESC).create());
        options.addOption(new OptionBuilder().withLongOpt(DELETE_SOURCE_OPTION).withDescription(DELETE_SOURCE_DESC).create());

        OptionGroup loggingOpts = new OptionGroup();
        loggingOpts.addOption(new OptionBuilder().withLongOpt(DEBUG_OPTION).withDescription(DEBUG_DESC).create());
        loggingOpts.addOption(new OptionBuilder().withLongOpt(VERBOSE_OPTION).withDescription(VERBOSE_DESC).create());
        loggingOpts.addOption(new OptionBuilder().withLongOpt(SILENT_OPTION).withDescription(SILENT_DESC).create());
        loggingOpts.addOption(new OptionBuilder().withLongOpt(QUIET_OPTION).withDescription(QUIET_DESC).create());
        options.addOptionGroup(loggingOpts);

        return options;
    }

    protected SyncSource<?> source;
    protected SyncTarget target;
    protected List<SyncFilter> filters = new ArrayList<>();
    protected int queryThreadCount = 2;
    protected int syncThreadCount = 2;
    protected boolean recursive = true;
    protected boolean timingsEnabled = false;
    protected int timingWindow = 10000;
    protected boolean rememberFailed = true;
    protected boolean deleteSource = false;
    protected String logLevel;

    protected CountingExecutor syncExecutor;
    protected CountingExecutor queryExecutor;
    protected SyncFilter firstFilter;
    protected boolean running;
    protected int completedCount, failedCount;
    protected long byteCount, startTime;
    protected Set<SyncObject> failedObjects;

    public void run() {
        // Some validation (must have source and target)
        Assert.notNull(source, "source plugin must be specified");
        Assert.notNull(target, "target plugin must be specified");

        // set log level before we do anything else
        if (logLevel != null) {
            switch (logLevel) {
                case DEBUG_OPTION:
                    LogManager.getRootLogger().setLevel(Level.DEBUG);
                    break;
                case VERBOSE_OPTION:
                    LogManager.getRootLogger().setLevel(Level.INFO);
                    break;
                case QUIET_OPTION:
                    LogManager.getRootLogger().setLevel(Level.WARN);
                    break;
                case SILENT_OPTION:
                    LogManager.getRootLogger().setLevel(Level.FATAL);
                    break;
            }
        }

        // filters are now fixed
        filters = Collections.unmodifiableList(filters);

        // Ask each plugin to validate the chain (resolves incompatible plugins)
        source.configure(source, filters.iterator(), target);
        target.configure(source, filters.iterator(), target);
        for (SyncFilter filter : filters) {
            filter.configure(source, filters.iterator(), target);
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
        if (previous != null) previous.setNext(target);

        firstFilter = filters.isEmpty() ? target : filters.get(0);

        // register for timings
        if (timingsEnabled) TimingUtil.register(this, timingWindow);

        // create thread pools
        queryExecutor = new CountingExecutor(queryThreadCount, queryThreadCount, 15, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        syncExecutor = new CountingExecutor(syncThreadCount, syncThreadCount, 15, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(syncThreadCount * 100));

        running = true;
        completedCount = 0;
        failedCount = 0;
        byteCount = 0;
        failedObjects = new HashSet<>();
        long intervalStart = startTime = System.currentTimeMillis();

        LogMF.info(l4j, "syncing from {0} to {1}", source.getSourceUri(), target.getTargetUri());

        // iterate through objects provided by source and submit tasks for syncing. these objects may have children,
        // in which case they will be submitted for crawling *after* they are synced.
        submitForSync(source);

        // now we must wait until all submitted tasks are complete
        while (running) {
            long now = System.currentTimeMillis();
            if (now - intervalStart > 60000) { // dump stats every minute
                LogMF.debug(l4j, "remaining tasks: {0}, active syncs: {1}, active queries: {2}",
                        syncExecutor.getRemainingTasks(), syncExecutor.getActiveCount(), queryExecutor.getActiveCount());
                LogMF.info(l4j, "completed Tasks: {0}, Failed Tasks: {1}",
                        completedCount, failedCount);
                intervalStart = now;
            }
            if (queryExecutor.getRemainingTasks() <= 0 && syncExecutor.getRemainingTasks() <= 0) {
                // done
                l4j.info("all tasks complete; shutting down");
                queryExecutor.shutdown();
                syncExecutor.shutdown();
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    l4j.warn("interrupted while sleeping", e);
                }
            }
        }

        // run a final timing log
        TimingUtil.logTimings(source);

        if (!running) { // terminated early
            l4j.warn("terminated early; forcing shutdown of thread pools");
            queryExecutor.shutdownNow();
            syncExecutor.shutdownNow();
        }
        running = false;

        // clean up any resources in the plugins
        cleanup();
    }

    public void terminate() {
        running = false;
    }

    public String getStatsString() {
        long secs = (System.currentTimeMillis() - startTime) / 1000L;
        if (secs == 0) secs = 1;
        long rate = byteCount / secs;
        double objrate = (double) completedCount / secs;

        return MessageFormat.format("Transferred {0} bytes in {1} seconds ({2} bytes/s)\n", byteCount, secs, rate) +
                MessageFormat.format("Successful files: {0} ({2,number,#.##}/s) Failed Files: {1}\n",
                        completedCount, failedCount, objrate) +
                MessageFormat.format("Failed files: {0}\n", failedObjects);
    }

    protected <T extends SyncObject<T>> void submitForQuery(SyncSource<T> syncSource, T syncObject) {
        queryExecutor.blockingSubmit(new QueryTask<T>(syncSource, syncObject));
    }

    protected <T extends SyncObject<T>> void submitForSync(SyncSource<T> syncSource, T syncObject) {
        syncExecutor.blockingSubmit(new SyncTask<T>(syncSource, syncObject));
    }

    protected <T extends SyncObject<T>> void submitForSync(SyncSource<T> syncSource) {
        for (T syncObject : syncSource) {
            if (!running) break;
            submitForSync(syncSource, syncObject);
        }
    }

    /**
     * updates the internal statistics for printing the summary at the end of execution.
     *
     * @param syncObject the SyncObject that has completed successfully.
     */
    protected synchronized void complete(SyncObject syncObject) {
        completedCount++;
        byteCount += syncObject.getBytesRead();
    }

    /**
     * updates the internal statistics for printing the summary at the end of execution.
     *
     * @param syncObject the object that has failed
     * @param t          the error that caused the failure.
     */
    protected synchronized void failed(SyncObject syncObject, Throwable t) {
        LogMF.warn(l4j, "O--! object {0} failed: {1}", syncObject, getCause(t));
        if (l4j.isDebugEnabled()) l4j.debug(summarize(t));
        failedCount++;
        if (rememberFailed) {
            failedObjects.add(syncObject);
        }
    }

    protected Throwable getCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) cause = cause.getCause();
        return cause;
    }

    protected String summarize(Throwable t) {
        Throwable cause = getCause(t);
        StringBuilder summary = new StringBuilder();
        summary.append(MessageFormat.format("[{0}] {1}", t, cause));
        StackTraceElement[] elements = cause.getStackTrace();
        for (int i = 0; i < 5 && i < elements.length; i++) {
            summary.append("\n    at ").append(elements[i]);
        }
        return summary.toString();
    }

    protected void cleanup() {
        source.cleanup();
        for (SyncFilter filter : filters) {
            filter.cleanup();
        }
        target.cleanup();
    }

    public SyncSource getSource() {
        return source;
    }

    /**
     * Sets the source plugin.
     */
    public void setSource(SyncSource source) {
        this.source = source;
    }

    public SyncTarget getTarget() {
        return target;
    }

    /**
     * Sets the target plugin.
     */
    public void setTarget(SyncTarget target) {
        this.target = target;
    }

    public List<SyncFilter> getFilters() {
        return filters;
    }

    /**
     * Sets the chain of filters to insert between the source and target.
     * This is used for Spring configuration.
     *
     * @param filters a list of filters to execute in between the source
     *                and target.
     */
    public void setFilters(List<SyncFilter> filters) {
        this.filters = filters;
    }

    public int getQueryThreadCount() {
        return queryThreadCount;
    }

    public void setQueryThreadCount(int queryThreadCount) {
        this.queryThreadCount = queryThreadCount;
    }

    public int getSyncThreadCount() {
        return syncThreadCount;
    }

    public void setSyncThreadCount(int syncThreadCount) {
        this.syncThreadCount = syncThreadCount;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public void setRecursive(boolean recursive) {
        this.recursive = recursive;
    }

    public boolean isTimingsEnabled() {
        return timingsEnabled;
    }

    /**
     * When set to true, enables operation timings on all plug-ins that support it. Default is false.
     */
    public void setTimingsEnabled(boolean timingsEnabled) {
        this.timingsEnabled = timingsEnabled;
    }

    public int getTimingWindow() {
        return timingWindow;
    }

    /**
     * Sets the window for timing statistics. Every {timingWindow} objects that are synced, timing statistics are logged
     * and reset. Default is 10,000 objects.
     */
    public void setTimingWindow(int timingWindow) {
        this.timingWindow = timingWindow;
    }

    public boolean isRememberFailed() {
        return rememberFailed;
    }

    public void setRememberFailed(boolean rememberFailed) {
        this.rememberFailed = rememberFailed;
    }

    public boolean isDeleteSource() {
        return deleteSource;
    }

    public void setDeleteSource(boolean deleteSource) {
        this.deleteSource = deleteSource;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public int getCompletedCount() {
        return completedCount;
    }

    public int getFailedCount() {
        return failedCount;
    }

    public long getByteCount() {
        return byteCount;
    }

    protected class QueryTask<T extends SyncObject<T>> implements Runnable {
        private SyncSource<T> syncSource;
        private T syncObject;

        public QueryTask(SyncSource<T> syncSource, T syncObject) {
            this.syncSource = syncSource;
            this.syncObject = syncObject;
        }

        @Override
        public void run() {
            try {
                if (syncObject.hasChildren()) {
                    LogMF.debug(l4j, ">>>> querying children of {0}", syncObject);
                    Iterator<T> children = syncObject.childIterator();
                    while (children.hasNext()) {
                        submitForSync(syncSource, children.next());
                    }
                    LogMF.debug(l4j, "<<<< finished querying children of {0}", syncObject);
                }
            } catch (Throwable t) {
                LogMF.warn(l4j, ">>!! querying children of {0} failed: {1}", syncObject, summarize(t));
            }
        }
    }

    protected class SyncTask<T extends SyncObject<T>> implements Runnable {
        private SyncSource<T> syncSource;
        private T syncObject;

        public SyncTask(SyncSource<T> syncSource, T syncObject) {
            this.syncSource = syncSource;
            this.syncObject = syncObject;
        }

        @Override
        public void run() {
            try {
                LogMF.debug(l4j, "O--+ syncing object {0}", syncObject);
                firstFilter.filter(syncObject);
                complete(syncObject);
                LogMF.debug(l4j, "O--O finished syncing object {0} ({1} bytes transferred)",
                        syncObject, syncObject.getBytesRead());

                try { // delete object if the source supports deletion (implements the delete() method)
                    if (deleteSource) {
                        syncSource.delete(syncObject);
                        LogMF.info(l4j, "X--O deleted {0} from source", syncObject);
                    }
                } catch (Throwable t) {
                    LogMF.warn(l4j, "!--O could not delete {0} from source", syncObject);
                }

                if (recursive && syncObject.hasChildren()) {
                    LogMF.debug(l4j, "{0} has children; submitting for query", syncObject);
                    submitForQuery(syncSource, syncObject);
                }
            } catch (Throwable t) {
                failed(syncObject, t);
            }
        }
    }

    protected static class CountingExecutor extends ThreadPoolExecutor {
        private final Object syncObject = new Object();
        private AtomicLong remainingTasks = new AtomicLong();

        public CountingExecutor(int corePoolSize, int maximumPoolSize,
                                long keepAliveTime, TimeUnit unit,
                                BlockingQueue<Runnable> workQueue) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        }

        public void blockingSubmit(Runnable task) {
            while (true) {
                if (this.isShutdown() || this.isTerminated() || this.isTerminating()) {
                    throw new RuntimeException("pool is not accepting tasks");
                }

                synchronized (syncObject) {
                    try {
                        this.submit(task);
                        remainingTasks.incrementAndGet();
                        return;
                    } catch (Exception e) {
                        LogMF.debug(l4j, "pool full trying to submit {0}.  Current size {1}, reason: {2}.",
                                task, this.getQueue().size(), e.getMessage());
                    }
                    if (this.isShutdown() || this.isTerminated() || this.isTerminating()) {
                        throw new RuntimeException("pool is not accepting tasks");
                    }
                    try {
                        syncObject.wait();
                    } catch (InterruptedException e) {
                        // Ignore
                    }
                }
            }

        }

        // A new task started.  The queue should be smaller.
        @Override
        protected void beforeExecute(Thread t, Runnable r) {
            synchronized (syncObject) {
                syncObject.notify();
            }
            super.beforeExecute(t, r);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            remainingTasks.decrementAndGet();
            super.afterExecute(r, t);
        }

        public long getRemainingTasks() {
            return remainingTasks.get();
        }
    }
}
