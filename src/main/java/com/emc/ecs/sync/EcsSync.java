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
package com.emc.ecs.sync;

import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectStatus;
import com.emc.ecs.sync.model.SyncEstimate;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.rest.RestServer;
import com.emc.ecs.sync.service.*;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.*;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;
import org.springframework.util.Assert;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * New plugin-based sync program.  Can be configured in two ways:
 * 1) through a command-line parser
 * 2) through Spring.  Call run() on the EcsSync object after your beans are
 * initialized.
 */
public class EcsSync implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(EcsSync.class);

    public static final String VERSION = EcsSync.class.getPackage().getImplementationVersion();

    private static final String VERSION_OPTION = "version";
    private static final String VERSION_DESC = "Displays package version";

    private static final String HELP_OPTION = "help";
    private static final String HELP_DESC = "Displays this help content";

    private static final String REST_ENDPOINT_OPTION = "rest-endpoint";
    private static final String REST_ENDPOINT_DESC = "Specified the host and port to use for the REST endpoint. Optional; defaults to " + RestServer.DEFAULT_HOST_NAME + ":" + RestServer.DEFAULT_PORT;
    private static final String REST_ENDPOINT_ARG_NAME = "hostname:port";

    private static final String REST_ONLY_OPTION = "rest-only";
    private static final String REST_ONLY_DESC = "Enables REST-only control. This will start the REST server and remain alive until manually terminated. Excludes all other options except " + REST_ENDPOINT_OPTION;

    private static final String ROOT_SPRING_BEAN = "sync";
    private static final String SPRING_CONFIG_OPTION = "spring-config";
    private static final String SPRING_CONFIG_DESC = "Specifies a Spring bean configuration file. In this mode, Spring is used to initialize the application configuration from a spring context XML file. It is assumed that there is a bean named '" + ROOT_SPRING_BEAN + "' containing a EcsSync object. This object will be initialized and executed. In this mode all other CLI arguments are ignored.";
    private static final String SPRING_CONFIG_ARG_NAME = "path-to-spring-file";

    private static final String SOURCE_OPTION = "source";
    private static final String SOURCE_DESC = "The URI for the synchronization source. Examples:\n" +
            "atmos:http://uid:secret@host:port\n" +
            " '- Uses Atmos as the source; could also be https.\n" +
            "file:///tmp/atmos/\n" +
            " '- Reads from a directory\n" +
            "archive:///tmp/atmos/backup.tar.gz\n" +
            " '- Reads from an archive file\n" +
            "s3:http://key:secret@host:port\n" +
            " '- Reads from an S3 bucket\n" +
            "Other plugins may be available. See their documentation for URI formats";
    private static final String SOURCE_ARG_NAME = "source-uri";

    private static final String TARGET_OPTION = "target";
    private static final String TARGET_DESC = "The URI for the synchronization target. Examples:\n" +
            "atmos:http://uid:secret@host:port\n" +
            " '- Uses Atmos as the target; could also be https.\n" +
            "file:///tmp/atmos/\n" +
            " '- Writes to a directory\n" +
            "archive:///tmp/atmos/backup.tar.gz\n" +
            " '- Writes to an archive file\n" +
            "s3:http://key:secret@host:port\n" +
            " '- Writes to an S3 bucket\n" +
            "Other plugins may be available. See their documentation for URI formats";
    private static final String TARGET_ARG_NAME = "target-uri";

    private static final String FILTERS_OPTION = "filters";
    private static final String FILTERS_DESC = "The comma-delimited list of filters to apply to objects as they are synced. " +
            "Specify the activation names of the filters [returned from Filter.getActivationName()]. Examples:\n" +
            "    id-logging\n" +
            "    db-id-mapping,strip-acls\n" +
            "Each filter may have additional custom parameters you may specify separately";
    private static final String FILTERS_ARG_NAME = "filter-names";

    private static final String QUERY_THREADS_OPTION = "query-threads";
    private static final String QUERY_THREADS_DESC = "Specifies the number of threads to use when querying for child objects";
    private static final String QUERY_THREADS_ARG_NAME = "thread-count";

    private static final String SYNC_THREADS_OPTION = "sync-threads";
    private static final String SYNC_THREADS_DESC = "Specifies the number of threads to use when syncing objects";
    private static final String SYNC_THREADS_ARG_NAME = "thread-count";

    private static final String TIMINGS_OPTION = "timing";
    private static final String TIMINGS_DESC = "Enables timings for all plug-ins that support it. When enabled, plug-ins will collect and periodically log average timing for various operations (i.e. read-metadata, stream-object, write-metadata, etc.)";

    private static final String TIMING_WINDOW_OPTION = "timing-window";
    private static final String TIMING_WINDOW_DESC = "Sets the timing window to use for timings. Every <window-size> objects, timing statistics will be averaged and logged";
    private static final String TIMING_WINDOW_ARG_NAME = "window-size";

    private static final String NON_RECURSIVE_OPTION = "non-recursive";
    private static final String NON_RECURSIVE_DESC = "By default, hierarchical sources will be synced recursively (i.e. Atmos source w/ namespace and filesystem source). Use this option to disable recursive behavior and sync only at the given level.";

    private static final String FORGET_FAILED_OPTION = "forget-failed";
    private static final String FORGET_FAILED_DESC = "By default, EcsSync tracks all failed objects and displays a summary of failures when finished. To save memory in large migrations, this option will disable this summary. If you use this option, be sure your logging is at an appropriate level and that you are capturing failures in a log file.";

    private static final String VERIFY_OPTION = "verify";
    private static final String VERIFY_DESC = "After a successful object transfer, the object will be read back from the target system and its MD5 checksum will be compared with that of the source object (generated during transfer). This only compares object data (metadata is not compared) and does not include directories.";

    private static final String VERIFY_ONLY_OPTION = "verify-only";
    private static final String VERIFY_ONLY_DESC = "Similar to --verify except that the object transfer is skipped and only read operations are performed (no data is written)";

    private static final String DELETE_SOURCE_OPTION = "delete-source";
    private static final String DELETE_SOURCE_DESC = "Supported source plugins will delete each source object once it is successfully synced (does not include directories). Use this option with care! Be sure log levels are appropriate to capture transferred (source deleted) objects.";

    private static final String RETRY_ATTEMPTS_OPTION = "retry-attempts";
    private static final String RETRY_ATTEMPTS_DESC = "Specifies how many times each object should be retried after an error. Default is 2";
    private static final String RETRY_ATTEMPTS_ARG_NAME = "num-retries";

    private static final String REPROCESS_OBJECTS_OPTION = "reprocess";
    private static final String REPROCESS_OBJECTS_DESC = "When using a database, specifies that all objects should be synced and or verified regardless of their status in the database. Normally, if an object is transferred or verified in the database, it will be skipped";

    private static final String REPORT_PERFORMANCE_OPTION = "report-performance";
    private static final String REPORT_PERFORMANCE_DESC = "Report upload and download rates for the source and target plugins every <x> seconds to INFO logging.  Default is off (-1)";

    private static final String SPLIT_POOLS_THRESHOLD_OPTION = "split-pools-threshold";
    private static final String SPLIT_POOLS_THRESHOLD_DESC = "Split sync threads into two pools; 1/4 of the threads will handle large objects (> byte-size), the rest will handle small objects (< byte-size)";
    private static final String SPLIT_POOLS_THRESHOLD_ARG_NAME = "byte-size";

    private static final String DB_FILE_OPTION = "db-file";
    private static final String DB_FILE_DESC = "Enables the Sqlite database engine and specifies the file to hold the status database. A database will make repeat runs and incrementals more efficient. You can also use the sqlite3 client to interrogate the details of all objects in the sync";
    private static final String DB_FILE_ARG_NAME = "database-file";

    private static final String DB_CONNECT_STRING_OPTION = "db-connect-string";
    private static final String DB_CONNECT_STRING_DESC = "Enables the MySQL database engine and specified the JDBC connect string to connect to the database (i.e. \"jdbc:mysql://localhost:3306/ecs_sync?user=foo&password=bar\")";
    private static final String DB_CONNECT_STRING_ARG_NAME = "jdbc-connect-string";

    private static final String DB_TABLE_OPTION = "db-table";
    private static final String DB_TABLE_DESC = "Specifies the DB table name to use. Use this with --" + DB_CONNECT_STRING_OPTION + " to provide a unique table name or risk corrupting a previously used table. Default table is " + DbService.DEFAULT_OBJECTS_TABLE_NAME;
    private static final String DB_TABLE_ARG_NAME = "table-name";

    // logging options
    private static final String DEBUG_OPTION = "debug";
    private static final String DEBUG_DESC = "Sets log threshold to DEBUG";
    private static final String VERBOSE_OPTION = "verbose";
    private static final String VERBOSE_DESC = "Sets log threshold to INFO";
    private static final String QUIET_OPTION = "quiet";
    private static final String QUIET_DESC = "Sets log threshold to WARNING";
    private static final String SILENT_OPTION = "silent";
    private static final String SILENT_DESC = "Disables logging";

    private static SafeLoader<SyncSource> sourceLoader = new SafeLoader<>(ServiceLoader.load(SyncSource.class));
    private static SafeLoader<SyncFilter> filterLoader = new SafeLoader<>(ServiceLoader.load(SyncFilter.class));
    private static SafeLoader<SyncTarget> targetLoader = new SafeLoader<>(ServiceLoader.load(SyncTarget.class));

    private static CommandLineParser parser = new DefaultParser();

    public static void main(String[] args) {
        System.out.println(versionLine());

        EcsSync sync;
        RestServer restServer;
        try {
            CommandLine line = parser.parse(mainOptions(), args, true);

            // configure logging for startup
            String cliLogLevel = QUIET_OPTION;
            if (line.hasOption(DEBUG_OPTION)) cliLogLevel = DEBUG_OPTION;
            else if (line.hasOption(VERBOSE_OPTION)) cliLogLevel = VERBOSE_OPTION;
            else if (line.hasOption(SILENT_OPTION)) cliLogLevel = SILENT_OPTION;
            configureLogLevel(cliLogLevel);

            // Special check for version
            if (line.hasOption(VERSION_OPTION)) {
                System.exit(0);
            }

            // Special check for help
            if (line.hasOption(HELP_OPTION)) {
                longHelp();
                System.exit(0);
            }

            // start REST service
            // first, hush up the JDK logger (why does this default to INFO??)
            java.util.logging.LogManager.getLogManager().getLogger("").setLevel(java.util.logging.Level.WARNING);
            if (line.hasOption(REST_ENDPOINT_OPTION)) {
                String[] endpoint = line.getOptionValue(REST_ENDPOINT_OPTION).split(":");
                restServer = new RestServer(endpoint[0], Integer.parseInt(endpoint[1]));
            } else {
                restServer = new RestServer();
                restServer.setAutoPortEnabled(true);
            }
            if (line.hasOption(DB_CONNECT_STRING_OPTION)) {
                SyncJobService.getInstance().setDbConnectString(line.getOptionValue(DB_CONNECT_STRING_OPTION));
            }
            restServer.start();

            // if REST-only, skip remaining logic (REST server thread will keep the VM running)
            if (line.hasOption(REST_ONLY_OPTION)) return;

            if (line.hasOption(SPRING_CONFIG_OPTION)) {

                // Spring configuration
                sync = springBootstrap(line.getOptionValue(SPRING_CONFIG_OPTION));
            } else {

                // CLI configuration
                sync = cliBootstrap(args);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.out.println("    use --help for a detailed (quite long) list of options");
            System.exit(1);
            return;
        }

        int exitCode = 0;
        try {
            SyncJobService.getInstance().registerJob(sync);
            sync.run();
            if (sync.getObjectsFailed() > 0) exitCode = 2;
        } catch (Throwable t) {
            t.printStackTrace();
            exitCode = 2;
        } finally {
            restServer.stop(0);
        }

        System.out.print(sync.getStatsString());

        System.exit(exitCode);
    }

    // Note: now that we use slf4j, this will *only* take effect if the log implementation is log4j
    private static void configureLogLevel(String logLevel) {
        if (logLevel == null) return;

        // try to avoid a runtime dependency on log4j (untested)
        try {
            org.apache.log4j.Logger rootLogger = org.apache.log4j.LogManager.getRootLogger();
            if (DEBUG_OPTION.equals(logLevel))
                rootLogger.setLevel(org.apache.log4j.Level.DEBUG);
            if (VERBOSE_OPTION.equals(logLevel))
                rootLogger.setLevel(org.apache.log4j.Level.INFO);
            if (QUIET_OPTION.equals(logLevel))
                rootLogger.setLevel(org.apache.log4j.Level.WARN);
            if (SILENT_OPTION.equals(logLevel))
                rootLogger.setLevel(org.apache.log4j.Level.ERROR);

            org.apache.log4j.AppenderSkeleton mainAppender = (org.apache.log4j.AppenderSkeleton) rootLogger.getAppender("mainAppender");
            org.apache.log4j.AppenderSkeleton stackAppender = (org.apache.log4j.AppenderSkeleton) rootLogger.getAppender("stacktraceAppender");
            if (DEBUG_OPTION.equals(logLevel) || VERBOSE_OPTION.equals(logLevel)) {
                if (mainAppender != null) mainAppender.setThreshold(org.apache.log4j.Level.OFF);
                if (stackAppender != null) stackAppender.setThreshold(org.apache.log4j.Level.ALL);
            } else {
                if (mainAppender != null) mainAppender.setThreshold(org.apache.log4j.Level.ALL);
                if (stackAppender != null) stackAppender.setThreshold(org.apache.log4j.Level.OFF);
            }
        } catch (Exception e) {
            log.warn("could not configure log4j (perhaps you're using a different logger, which is fine)", e);
        }
    }

    /**
     * Initializes a Spring Application Context from the given file and
     * bootstraps the EcsSync object from there.
     */
    protected static EcsSync springBootstrap(String pathToSpringXml) {
        File springXml = new File(pathToSpringXml);
        if (!springXml.exists()) {
            throw new ConfigurationException("the Spring XML file: " + springXml + " does not exist");
        }

        log.info("loading configuration from Spring XML file: " + springXml);
        FileSystemXmlApplicationContext ctx =
                new FileSystemXmlApplicationContext(springXml.toURI().toString());

        if (!ctx.containsBean(ROOT_SPRING_BEAN)) {
            throw new ConfigurationException("your Spring XML file: " + springXml + " must contain one bean named '" +
                    ROOT_SPRING_BEAN + "' that initializes an EcsSync object");
        }

        return ctx.getBean(ROOT_SPRING_BEAN, EcsSync.class);
    }

    /**
     * Loads and configures plugins based on command line options.
     */
    protected static EcsSync cliBootstrap(String[] args) throws ParseException {
        EcsSync sync = new EcsSync();
        List<SyncPlugin> plugins = new ArrayList<>();

        CommandLine line = parser.parse(allOptions(), args, false);

        // find a plugin that can read from the source
        String sourceUri = line.getOptionValue(SOURCE_OPTION);
        if (sourceUri != null) {
            for (SyncSource source : sourceLoader) {
                if (source.canHandleSource(sourceUri)) {
                    source.setSourceUri(sourceUri);
                    sync.setSource(source);
                    plugins.add(source);
                    log.info("source: {} ({})", source.getName(), source.getClass());
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
                    log.info("target: {} ({})", target.getName(), target.getClass());
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
                        log.info("filter: {} ({})", filter.getName(), filter.getClass());
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

        if (line.hasOption(VERIFY_OPTION)) sync.setVerify(true);
        if (line.hasOption(VERIFY_ONLY_OPTION)) sync.setVerifyOnly(true);

        // configure whether to delete source objects after they are successfully synced
        if (line.hasOption(DELETE_SOURCE_OPTION)) sync.setDeleteSource(true);

        if (line.hasOption(RETRY_ATTEMPTS_OPTION))
            sync.setRetryAttempts(Integer.parseInt(line.getOptionValue(RETRY_ATTEMPTS_OPTION)));

        if (line.hasOption(REPROCESS_OBJECTS_OPTION)) sync.setReprocessObjects(true);

        if(line.hasOption(REPORT_PERFORMANCE_OPTION)) {
            sync.setReportPerformance(Integer.parseInt(line.getOptionValue(REPORT_PERFORMANCE_OPTION)));
        }

        if (line.hasOption(SPLIT_POOLS_THRESHOLD_OPTION))
            sync.setSplitPoolsThreshold(Integer.parseInt(line.getOptionValue(SPLIT_POOLS_THRESHOLD_OPTION)));

        if (line.hasOption(DB_FILE_OPTION)) sync.setDbFile(line.getOptionValue(DB_FILE_OPTION));

        if (line.hasOption(DB_CONNECT_STRING_OPTION))
            sync.setDbConnectString(line.getOptionValue(DB_CONNECT_STRING_OPTION));

        if (line.hasOption(DB_TABLE_OPTION)) sync.setDbTable(line.getOptionValue(DB_TABLE_OPTION));

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

        if (log.isDebugEnabled()) {
            for (Option option : line.getOptions()) {
                if (option.hasArg())
                    log.debug("parsed option {}: {}", option.getLongOpt(), line.getOptionValue(option.getLongOpt()));
                else
                    log.debug("parsed option {}", option.getLongOpt());
            }
        }

        // Let the plugins parse their own options
        for (SyncPlugin plugin : plugins) {
            plugin.parseOptions(line);
        }

        return sync;
    }

    protected static void longHelp() {
        HelpFormatter fmt = new HelpFormatter();
        fmt.setWidth(79);

        Options options = mainOptions();
        for (Object o : CommonOptions.getOptions().getOptions()) {
            options.addOption((Option) o);
        }

        // Make sure we do CommonOptions first
        fmt.printHelp("java -jar ecs-sync.jar -source <source-uri> [-filters <filter1>[,<filter2>,...]] -target <target-uri> [options]\n" +
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
        options.addOption(Option.builder().longOpt(VERSION_OPTION).desc(VERSION_DESC).build());
        options.addOption(Option.builder().longOpt(HELP_OPTION).desc(HELP_DESC).build());
        options.addOption(Option.builder().longOpt(SPRING_CONFIG_OPTION).desc(SPRING_CONFIG_DESC)
                .hasArg().argName(SPRING_CONFIG_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(QUERY_THREADS_OPTION).desc(QUERY_THREADS_DESC)
                .hasArg().argName(QUERY_THREADS_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(SYNC_THREADS_OPTION).desc(SYNC_THREADS_DESC)
                .hasArg().argName(SYNC_THREADS_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(SOURCE_OPTION).desc(SOURCE_DESC)
                .hasArg().argName(SOURCE_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(TARGET_OPTION).desc(TARGET_DESC)
                .hasArg().argName(TARGET_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(FILTERS_OPTION).desc(FILTERS_DESC)
                .hasArg().argName(FILTERS_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(NON_RECURSIVE_OPTION).desc(NON_RECURSIVE_DESC).build());
        options.addOption(Option.builder().longOpt(TIMINGS_OPTION).desc(TIMINGS_DESC).build());
        options.addOption(Option.builder().longOpt(TIMING_WINDOW_OPTION).desc(TIMING_WINDOW_DESC)
                .hasArg().argName(TIMING_WINDOW_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(FORGET_FAILED_OPTION).desc(FORGET_FAILED_DESC).build());
        options.addOption(Option.builder().longOpt(VERIFY_OPTION).desc(VERIFY_DESC).build());
        options.addOption(Option.builder().longOpt(VERIFY_ONLY_OPTION).desc(VERIFY_ONLY_DESC).build());
        options.addOption(Option.builder().longOpt(DELETE_SOURCE_OPTION).desc(DELETE_SOURCE_DESC).build());
        options.addOption(Option.builder().longOpt(REST_ONLY_OPTION).desc(REST_ONLY_DESC).build());
        options.addOption(Option.builder().longOpt(REST_ENDPOINT_OPTION).desc(REST_ENDPOINT_DESC)
                .hasArg().argName(REST_ENDPOINT_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(RETRY_ATTEMPTS_OPTION).desc(RETRY_ATTEMPTS_DESC)
                .hasArg().argName(RETRY_ATTEMPTS_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(REPROCESS_OBJECTS_OPTION).desc(REPROCESS_OBJECTS_DESC).build());
        options.addOption(Option.builder().longOpt(REPORT_PERFORMANCE_OPTION).desc(REPORT_PERFORMANCE_DESC)
                .hasArg().argName("seconds").build());
        options.addOption(Option.builder().longOpt(SPLIT_POOLS_THRESHOLD_OPTION).desc(SPLIT_POOLS_THRESHOLD_DESC)
                .hasArg().argName(SPLIT_POOLS_THRESHOLD_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(DB_FILE_OPTION).desc(DB_FILE_DESC)
                .hasArg().argName(DB_FILE_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(DB_CONNECT_STRING_OPTION).desc(DB_CONNECT_STRING_DESC)
                .hasArg().argName(DB_CONNECT_STRING_ARG_NAME).build());
        options.addOption(Option.builder().longOpt(DB_TABLE_OPTION).desc(DB_TABLE_DESC)
                .hasArg().argName(DB_TABLE_ARG_NAME).build());

        OptionGroup loggingOpts = new OptionGroup();
        loggingOpts.addOption(Option.builder().longOpt(DEBUG_OPTION).desc(DEBUG_DESC).build());
        loggingOpts.addOption(Option.builder().longOpt(VERBOSE_OPTION).desc(VERBOSE_DESC).build());
        loggingOpts.addOption(Option.builder().longOpt(SILENT_OPTION).desc(SILENT_DESC).build());
        loggingOpts.addOption(Option.builder().longOpt(QUIET_OPTION).desc(QUIET_DESC).build());
        options.addOptionGroup(loggingOpts);

        return options;
    }

    protected static Options allOptions() {
        Options options = mainOptions();

        // common plugin options
        for (Object o : CommonOptions.getOptions().getOptions()) {
            options.addOption((Option) o);
        }

        // dynamic plugin custom options
        List<SyncPlugin> plugins = new ArrayList<>();
        for (SyncPlugin plugin : sourceLoader) {
            plugins.add(plugin);
        }
        for (SyncPlugin plugin : targetLoader) {
            plugins.add(plugin);
        }
        for (SyncPlugin plugin : filterLoader) {
            plugins.add(plugin);
        }

        for (SyncPlugin plugin : plugins) {
            for (Object o : plugin.getCustomOptions().getOptions()) {
                Option option = (Option) o;
                if (options.hasOption(option.getOpt())) {
                    log.warn("The option " + option.getOpt() + " is being used by more than one plugin");
                }
                options.addOption(option);
            }
        }

        return options;
    }

    protected static String versionLine() {
        return EcsSync.class.getSimpleName() + (VERSION == null ? "" : " v" + VERSION);
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
    protected boolean verify = false;
    protected boolean verifyOnly = false;
    protected boolean deleteSource = false;
    protected String logLevel;
    protected int retryAttempts = 2;
    protected DbService dbService;
    protected String dbFile;
    protected String dbConnectString;
    protected String dbTable;
    protected boolean reprocessObjects = false;
    protected int splitPoolsThreshold;
    protected Throwable runError;

    protected EnhancedThreadPoolExecutor syncExecutor;
    protected EnhancedThreadPoolExecutor largeSyncExecutor;
    protected EnhancedThreadPoolExecutor queryExecutor;
    protected EnhancedThreadPoolExecutor retrySubmitter;
    protected SyncFilter firstFilter;
    protected SyncEstimate syncEstimate;
    protected Future estimateTask;
    protected boolean running, paused, terminated;
    protected int objectsComplete, objectsFailed;
    protected long bytesComplete, pastRunTime, startTime, stopTime, cpuStartTime;
    protected Set<SyncObject> failedObjects;
    protected int reportPerformance = -1;
    protected ScheduledExecutorService performanceReporter;
    protected PerformanceWindow objectCompleteRate = SyncPlugin.defaultPerformanceWindow();
    protected PerformanceWindow objectErrorRate = SyncPlugin.defaultPerformanceWindow();

    public void run() {
        try {
            // set log level before we do anything else
            configureLogLevel(logLevel);

            log.info("Sync started at " + new Date());
            startTime = System.currentTimeMillis();
            cpuStartTime = ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() / 1000000;

            // Summarize config for reference
            if (log.isInfoEnabled()) log.info(summarizeConfig());

            // Some validation (must have source and target)
            Assert.notNull(source, "source plugin must be specified");
            Assert.notNull(target, "target plugin must be specified");

            // filters are now fixed
            filters = Collections.unmodifiableList(filters);

            // Ask each plugin to configure itself and validate the chain (resolves incompatible plugins)
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

            // start estimating in the background
            ExecutorService estimateExecutor = Executors.newSingleThreadExecutor();
            estimateTask = estimateExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    syncEstimate = source.createEstimate();
                }
            });
            estimateExecutor.shutdown();

            // initialize DB Service if necessary
            if (dbService == null) {
                if (dbFile != null) {
                    dbService = new SqliteDbService(dbFile);
                } else if (dbConnectString != null) {
                    dbService = new MySQLDbService(dbConnectString, null, null);
                } else {
                    dbService = new NoDbService();
                }
                if (dbTable != null) dbService.setObjectsTableName(dbTable);
            }
            dbService.setTimingPlugin(source);

            // create thread pools
            queryExecutor = new EnhancedThreadPoolExecutor(queryThreadCount, new LinkedBlockingDeque<Runnable>(), "query-pool");
            // if requested, split the pools for large and small objects for better average efficiency
            if (splitPoolsThreshold > 0) {
                largeSyncExecutor = new EnhancedThreadPoolExecutor(syncThreadCount / 4, new LinkedBlockingDeque<Runnable>(syncThreadCount * 25), "large-pool");
                syncExecutor = new EnhancedThreadPoolExecutor(3 * syncThreadCount / 4, new LinkedBlockingDeque<Runnable>(syncThreadCount * 75), "sync-pool");
            } else {
                largeSyncExecutor = new EnhancedThreadPoolExecutor(1, new LinkedBlockingDeque<Runnable>(1), "large-pool");
                syncExecutor = new EnhancedThreadPoolExecutor(syncThreadCount, new LinkedBlockingDeque<Runnable>(syncThreadCount * 100), "sync-pool");
            }
            retrySubmitter = new EnhancedThreadPoolExecutor(syncThreadCount, new LinkedBlockingDeque<Runnable>(), "retry-submitter");

            running = true;
            objectsComplete = objectsFailed = 0;
            bytesComplete = 0;
            failedObjects = new HashSet<>();

            log.info("syncing from {} to {}", source.getSourceUri(), target.getTargetUri());

            if (reportPerformance != -1) {
                performanceReporter = Executors.newSingleThreadScheduledExecutor();
                performanceReporter.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        log.info("Source: read: {} b/s write: {} b/s", source.getReadPerformance(),
                                source.getWritePerformance());
                        log.info("Target: read: {} b/s write: {} b/s", target.getReadPerformance(),
                                target.getWritePerformance());
                        log.info("Objects: complete: {}/s failed: {}/s", getObjectCompleteRate(), getObjectErrorRate());
                    }
                }, reportPerformance, reportPerformance, TimeUnit.SECONDS);
            }

            // iterate through objects provided by source and submit tasks for syncing and crawling (querying).
            submitForSync(source);

            // now we must wait until all submitted tasks are complete
            while (running) {
                if (queryExecutor.getUnfinishedTasks() <= 0 && syncExecutor.getUnfinishedTasks() <= 0
                        && largeSyncExecutor.getUnfinishedTasks() <= 0) {
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
            TimingUtil.logTimings(source);
        } catch (Throwable t) {
            log.error("unexpected exception", t);
            runError = t;
            throw t;
        } finally {
            if (!running) log.warn("terminated early!");
            running = false;
            if (paused) {
                paused = false;
                // must interrupt the threads that are blocked
                if (queryExecutor != null) queryExecutor.shutdownNow();
                if (retrySubmitter != null) retrySubmitter.shutdownNow();
                if (syncExecutor != null) syncExecutor.shutdownNow();
                if (largeSyncExecutor != null) largeSyncExecutor.shutdownNow();
            } else {
                if (queryExecutor != null) queryExecutor.shutdown();
                if (retrySubmitter != null) retrySubmitter.shutdown();
                if (syncExecutor != null) syncExecutor.shutdown();
                if (largeSyncExecutor != null) largeSyncExecutor.shutdown();
            }
            stopTime = System.currentTimeMillis();

            // clean up any resources in the plugins
            cleanup();
        }
    }

    /**
     * Stops the underlying executors from executing new tasks. Currently running tasks will complete and all threads
     * will then block until resumed
     *
     * @return true if the state was changed from running to pause; false if already paused
     * @throws IllegalStateException if the sync is complete or was terminated
     */
    public boolean pause() {
        if (!running) throw new IllegalStateException("sync is not running");
        boolean changed = queryExecutor.pause() && syncExecutor.pause() && largeSyncExecutor.pause();
        paused = true;
        stopTime = System.currentTimeMillis();
        return changed;
    }

    /**
     * Resumes the underlying executors so they may continue to execute tasks
     *
     * @return true if the state was changed from paused to running; false if already running
     * @throws IllegalStateException if the sync is complete or was terminated
     * @see #pause()
     */
    public boolean resume() {
        if (!running) throw new IllegalStateException("sync is not running");
        boolean changed = queryExecutor.resume() && syncExecutor.resume() && largeSyncExecutor.resume();
        paused = false;
        pastRunTime += stopTime - startTime;
        startTime = System.currentTimeMillis();
        stopTime = 0;
        return changed;
    }

    public void terminate() {
        running = false;
        terminated = true;
        queryExecutor.getQueue().clear();
        retrySubmitter.getQueue().clear();
    }

    public String summarizeConfig() {
        StringBuilder summary = new StringBuilder("Configuration Summary:\n");
        summary.append(getClass().getSimpleName()).append(":\n");
        summary.append(" - queryThreadCount: ").append(queryThreadCount).append("\n");
        summary.append(" - syncThreadCount: ").append(syncThreadCount).append("\n");
        summary.append(" - recursive: ").append(recursive).append("\n");
        summary.append(" - timingsEnabled: ").append(timingsEnabled).append("\n");
        summary.append(" - timingWindow: ").append(timingWindow).append("\n");
        summary.append(" - rememberFailed: ").append(rememberFailed).append("\n");
        summary.append(" - verify: ").append(verify).append("\n");
        summary.append(" - verifyOnly: ").append(verifyOnly).append("\n");
        summary.append(" - deleteSource: ").append(deleteSource).append("\n");
        summary.append(" - logLevel: ").append(logLevel).append("\n");
        summary.append(" - retryAttempts: ").append(retryAttempts).append("\n");
        summary.append(" - dbService: ").append(dbService).append("\n");
        summary.append(" - dbFile: ").append(dbFile).append("\n");
        summary.append(" - dbConnectString: ").append(dbConnectString).append("\n");
        summary.append(" - dbTable: ").append(dbTable).append("\n");
        summary.append(" - reprocessObjects: ").append(reprocessObjects).append("\n");
        summary.append(" - splitPoolsThreshold: ").append(splitPoolsThreshold).append("\n");
        summary.append("Source: ").append(source.summarizeConfig());
        summary.append("Target: ").append(target.summarizeConfig());
        for (SyncFilter filter : filters) {
            summary.append("Filter: ").append(filter.summarizeConfig());
        }
        return summary.toString();
    }

    public String getStatsString() {
        long secs = (System.currentTimeMillis() - startTime) / 1000L;
        if (secs == 0) secs = 1;
        long byteRate = bytesComplete / secs;
        double objectRate = (double) objectsComplete / secs;

        return MessageFormat.format("Transferred {0} bytes in {1} seconds ({2} bytes/s)\n", bytesComplete, secs, byteRate) +
                MessageFormat.format("Successful files: {0} ({2,number,#.##}/s) Failed Files: {1}\n",
                        objectsComplete, objectsFailed, objectRate) +
                MessageFormat.format("Failed files: {0}\n", failedObjects);
    }

    protected <T extends SyncObject<T>> void submitForQuery(SyncSource<T> syncSource, T syncObject) {
        if (running) queryExecutor.blockingSubmit(new QueryTask<>(syncSource, syncObject));
        else log.debug("not submitting task for query because terminate() was called: " + syncObject);
    }

    protected <T extends SyncObject<T>> void submitForSync(SyncSource<T> syncSource, T syncObject) {
        if (running) {
            if (splitPoolsThreshold > 0 && syncObject.isLargeObject(splitPoolsThreshold))
                largeSyncExecutor.blockingSubmit(new SyncTask<>(syncSource, syncObject));
            else syncExecutor.blockingSubmit(new SyncTask<>(syncSource, syncObject));
        } else {
            log.debug("not submitting task for sync because terminate() was called: " + syncObject);
        }
    }

    protected <T extends SyncObject<T>> void submitForSync(SyncSource<T> syncSource) {
        for (T syncObject : syncSource) {
            if (!running) break;
            submitForSync(syncSource, syncObject);
            submitForQuery(syncSource, syncObject);
        }
    }

    protected <T extends SyncObject<T>> void submitForRetry(final SyncSource<T> syncSource, final T syncObject, Throwable t)
            throws Throwable {
        syncObject.incFailureCount();
        if (syncObject.getFailureCount() > retryAttempts) throw t;

        // prepare for retry
        try {
            syncObject.reset();
            log.warn("O--R object {} failed {} time{} (queuing for retry): {}",
                    syncObject, syncObject.getFailureCount(), syncObject.getFailureCount() > 1 ? "s" : "",
                    SyncUtil.getCause(t));
            dbService.setStatus(syncObject, ObjectStatus.RetryQueue, SyncUtil.summarize(t), false);

            retrySubmitter.submit(new Runnable() {
                @Override
                public void run() {
                    submitForSync(syncSource, syncObject);
                }
            });
        } catch (Throwable t2) {
            // could not retry, so bubble original error
            log.warn("retry for {} failed: {}", syncObject, SyncUtil.getCause(t2));
            throw t;
        }
    }

    /**
     * updates the overall sync statistics
     */
    protected synchronized void syncComplete(SyncObject syncObject) {
        objectsComplete++;
        bytesComplete += syncObject.getBytesRead();
        objectCompleteRate.increment(1);
    }

    /**
     * updates the internal statistics for printing the summary at the end of execution.
     *
     * @param syncObject the object that has failed
     * @param t          the error that caused the failure.
     */
    protected synchronized void failed(SyncObject syncObject, Throwable t) {
        log.warn("O--! object " + syncObject + " failed", SyncUtil.getCause(t));
        objectsFailed++;
        if (rememberFailed) {
            failedObjects.add(syncObject);
        }
        objectErrorRate.increment(1);
    }

    protected void cleanup() {
        if(performanceReporter != null) {
            // Stop performance reporting thread.
            performanceReporter.shutdownNow();
        }
        objectCompleteRate.close();
        objectErrorRate.close();
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
        if (queryExecutor != null) queryExecutor.resizeThreadPool(queryThreadCount);
    }

    public int getSyncThreadCount() {
        return syncThreadCount;
    }

    public void setSyncThreadCount(int syncThreadCount) {
        this.syncThreadCount = syncThreadCount;
        if (syncExecutor != null) {
            if (splitPoolsThreshold > 0 && largeSyncExecutor != null) {
                largeSyncExecutor.resizeThreadPool(syncThreadCount / 4);
                syncExecutor.resizeThreadPool(3 * syncThreadCount / 4);
            } else {
                syncExecutor.resizeThreadPool(syncThreadCount);
            }
        }
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

    public boolean isVerify() {
        return verify;
    }

    public void setVerify(boolean verify) {
        this.verify = verify;
    }

    public boolean isVerifyOnly() {
        return verifyOnly;
    }

    public void setVerifyOnly(boolean verifyOnly) {
        this.verifyOnly = verifyOnly;
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

    public int getRetryAttempts() {
        return retryAttempts;
    }

    public void setRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
    }

    public boolean isReprocessObjects() {
        return reprocessObjects;
    }

    public void setReprocessObjects(boolean reprocessObjects) {
        this.reprocessObjects = reprocessObjects;
    }

    public int getSplitPoolsThreshold() {
        return splitPoolsThreshold;
    }

    /**
     * Splits the thread count into two thread pools; 1/4 of the threads will handle large objects (&gt;
     * <code>splitPoolsThreshold</code>), the rest will handle small objects (&lt; <code>splitPoolsThreshold</code>), as
     * determined by the SyncObject implementation. Default is 0 (disabled)
     *
     * @see SyncObject#isLargeObject(int)
     */
    public void setSplitPoolsThreshold(int splitPoolsThreshold) {
        this.splitPoolsThreshold = splitPoolsThreshold;
    }

    public DbService getDbService() {
        return dbService;
    }

    public void setDbService(DbService dbService) {
        this.dbService = dbService;
    }

    public String getDbFile() {
        return dbFile;
    }

    public void setDbFile(String dbFile) {
        this.dbFile = dbFile;
    }

    public String getDbConnectString() {
        return dbConnectString;
    }

    public void setDbConnectString(String dbConnectString) {
        this.dbConnectString = dbConnectString;
    }

    public String getDbTable() {
        return dbTable;
    }

    public void setDbTable(String dbTable) {
        this.dbTable = dbTable;
    }

    public Throwable getRunError() {
        return runError;
    }

    public int getObjectsComplete() {
        return objectsComplete;
    }

    public long getBytesComplete() {
        return bytesComplete;
    }

    public int getObjectsFailed() {
        return objectsFailed;
    }

    public Set<SyncObject> getFailedObjects() {
        return failedObjects;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public long getTotalRunTime() {
        if (startTime == 0) return 0;
        long last = stopTime > 0 ? stopTime : System.currentTimeMillis();
        return pastRunTime + (last - startTime);
    }

    /**
     * Returns the CPU time consumed by this sync process in milliseconds
     */
    public long getTotalCpuTime() {
        if (cpuStartTime == 0) return 0;
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() / 1000000 - cpuStartTime;

    }

    public boolean isRunning() {
        return running;
    }

    public boolean isPaused() {
        return paused;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public boolean isEstimating() {
        return estimateTask != null && !estimateTask.isDone();
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
        if (largeSyncExecutor != null) count += largeSyncExecutor.getActiveCount();
        return count;
    }

    public int getReportPerformance() {
        return reportPerformance;
    }

    public void setReportPerformance(int reportPerformance) {
        this.reportPerformance = reportPerformance;
    }

    public long getObjectCompleteRate() {
        return objectCompleteRate.getWindowRate();
    }

    public long getObjectErrorRate() {
        return objectErrorRate.getWindowRate();
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
            if (!running) {
                log.debug("aborting query task because terminate() was called: " + syncObject);
                return;
            }
            try {
                if (syncObject.isDirectory()) {
                    log.debug(">>>> querying children of {}", syncObject);
                    Iterator<T> children = syncSource.childIterator(syncObject);
                    while (children.hasNext()) {
                        T child = children.next();
                        submitForSync(syncSource, child);

                        if (recursive && child.isDirectory()) {
                            log.debug("{} is directory; submitting for query", child);
                            submitForQuery(syncSource, child);
                        }
                    }
                    log.debug("<<<< finished querying children of {}", syncObject);
                }
            } catch (Throwable t) {
                log.warn(">>!! querying children of " + syncObject + " failed: {}", t);
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
            if (!running) {
                log.debug("aborting sync task because terminate() was called: " + syncObject);
                return;
            }
            if (syncSource.veto(syncObject)) {
                log.debug("source vetoed {}", syncObject);
                return;
            }
            boolean processed = false, newRecord = false;
            SyncRecord record;
            try {
                record = dbService.getSyncRecord(syncObject);
                if (record != null && record.getTargetId() != null)
                    syncObject.setTargetIdentifier(record.getTargetId());
                newRecord = record == null;

                // truncate milliseconds (the DB only stores to the second)
                Date mtime = new Date(syncObject.getMetadata().getModificationTime().getTime() / 1000 * 1000);

                if (!verifyOnly) {
                    if (reprocessObjects || record == null || !ObjectStatus.isFinal(record.getStatus())
                            || (mtime != null && record.getMtime() != null && mtime.after(record.getMtime()))) {

                        log.debug("O--+ syncing {} {}", syncObject.isDirectory() ? "directory" : "object", syncObject);
                        dbService.setStatus(syncObject, ObjectStatus.InTransfer, null, newRecord);
                        newRecord = false;
                        try {
                            syncSource.sync(syncObject, firstFilter);
                        } catch (Throwable t) {
                            submitForRetry(syncSource, syncObject, t);
                            return;
                        }

                        if (syncObject.isDirectory()) log.info("O--O finished syncing directory {}", syncObject);
                        else log.info("O--O finished syncing object {} ({} bytes transferred)",
                                syncObject, syncObject.getBytesRead());
                        dbService.setStatus(syncObject, ObjectStatus.Transferred, null, false);
                        processed = true;
                    }
                }

                if (verify || verifyOnly) {
                    if (reprocessObjects || record == null || record.getStatus() != ObjectStatus.Verified
                            || (mtime != null && record.getMtime() != null && mtime.after(record.getMtime()))) {

                        log.debug("O==? verifying {} {}", syncObject, syncObject.isDirectory() ? "directory" : "object");
                        dbService.setStatus(syncObject, ObjectStatus.InVerification, null, newRecord);
                        newRecord = false;
                        try {
                            syncSource.verify(syncObject, firstFilter);
                        } catch (Throwable t) {
                            if (!verifyOnly) { // if we just copied the data and verification failed, we should retry
                                submitForRetry(syncSource, syncObject, t);
                                return;
                            } else throw t;
                        }

                        log.info("O==O verification successful for {}", syncObject);
                        dbService.setStatus(syncObject, ObjectStatus.Verified, null, false);
                        processed = true;
                    }
                }

                if (processed) syncComplete(syncObject);

                try { // delete object if the source supports deletion (implements the delete() method)
                    if (deleteSource) {
                        syncSource.delete(syncObject);
                        log.info("X--O deleted {} from source", syncObject);
                    }
                } catch (Throwable t) {
                    log.warn("!--O could not delete {} from source: {}", syncObject, t);
                }
            } catch (Throwable t) {
                try {
                    dbService.setStatus(syncObject, ObjectStatus.Error, SyncUtil.summarize(t), newRecord);
                } catch (Throwable t2) {
                    log.warn("error setting DB status", t2);
                }
                failed(syncObject, t);
            }
        }
    }

    private static class SafeLoader<T> implements Iterable<T> {
        private ServiceLoader<T> serviceLoader;

        public SafeLoader(ServiceLoader<T> serviceLoader) {
            this.serviceLoader = serviceLoader;
        }

        @Override
        public Iterator<T> iterator() {
            return new SafeIterator<>(serviceLoader.iterator());
        }
    }

    private static class SafeIterator<T> implements Iterator<T> {
        private Iterator<T> delegate;
        private T nextThing;

        public SafeIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (nextThing != null) return true;
            while (delegate.hasNext()) {
                try {
                    nextThing = delegate.next();
                    return true;
                } catch (ServiceConfigurationError e) {
                    if (e.getCause() instanceof UnsupportedClassVersionError) {
                        String plugin = e.getMessage();
                        try {
                            plugin = plugin.substring(plugin.indexOf("Provider ") + 9).split(" ")[0];
                            plugin = plugin.substring(plugin.lastIndexOf(".") + 1);
                        } catch (Throwable t) {
                            // ignore
                        }
                        log.warn("the {} plugin is not supported in this version of java", plugin);
                    } else throw e;
                }
            }
            return false;
        }

        @Override
        public T next() {
            if (nextThing == null) throw new NoSuchElementException();
            T returnThing = nextThing;
            nextThing = null;
            return returnThing;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("this iterator does not support removal");
        }
    }
}
