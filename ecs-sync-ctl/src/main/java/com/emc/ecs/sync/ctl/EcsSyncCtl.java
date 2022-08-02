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
package com.emc.ecs.sync.ctl;

import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.XmlGenerator;
import com.emc.ecs.sync.rest.*;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Entry point class for the ECS Sync CLI
 */
public class EcsSyncCtl {
    private static final Logger log = LogManager.getLogger(EcsSyncCtl.class);
    private static final String DEBUG_OPT = "debug";
    private static final String VERBOSE_OPT = "verbose";

    private static final String PAUSE_OPT = "pause";
    private static final String RESUME_OPT = "resume";
    private static final String STOP_OPT = "stop";
    private static final String DELETE_OPT = "delete";
    private static final String STATUS_OPT = "status";
    private static final String SUBMIT_OPT = "submit";
    private static final String SET_THREADS_OPT = "set-threads";
    private static final String THREADS_OPT = "threads";
    private static final String LOG_FILE_OPT = "log-file";
    private static final String LOG_PATTERN_OPT = "log-pattern";
    private static final String LIST_JOBS_OPT = "list-jobs";
    private static final String ENDPOINT_OPT = "endpoint";
    private static final String XML_GEN_OPT = "xml-gen";
    private static final String XG_SOURCE_OPT = "xml-source";
    private static final String XG_TARGET_OPT = "xml-target";
    private static final String XG_FILTERS_OPT = "xml-filters";
    private static final String XG_COMMENTS_OPT = "xml-comments";
    private static final String XG_SIMPLE_OPT = "xml-simple";
    private static final String HOST_INFO_OPT = "host-info";
    private static final String SET_LOG_LEVEL_OPT = "set-log-level";

    private static final String DEFAULT_ENDPOINT = "http://localhost:9200";
    private static final String JAR_NAME = "ecs-sync-ctl-{version}";

    private static final String LAYOUT_STRING_FILE = "%d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n";
    private static final String LAYOUT_STRING_CONSOLE = "%d{MM-dd HH:mm:ss}%-5p [%t] %c{1}:%L - %m%n";
    private static final String LAYOUT_STRING_CONSOLE_NOANSI = "%d{MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n";

    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_ARG_ERROR = 255;
    private static final int EXIT_FILE_NOT_FOUND = 2;
    private static final int EXIT_LOGGER_ERROR = 3;
    private static final int EXIT_JOB_CONFLICT = 4;
    private static final int EXIT_NO_JOB = 5;
    private static final int EXIT_NO_SERVICE = 6;
    private static final int EXIT_UNKNOWN_ERROR = 99;

    public static void main(String[] args) {
        Options opts = new Options();

        OptionGroup commands = new OptionGroup();
        commands.addOption(Option.builder().longOpt(PAUSE_OPT).hasArg().argName("job-id")
                .desc("Pauses the specified sync job").build());
        commands.addOption(Option.builder().longOpt(RESUME_OPT).hasArg().argName("job-id")
                .desc("Resumes a specified sync job").build());
        commands.addOption(Option.builder().longOpt(STOP_OPT).hasArg().argName("job-id")
                .desc("Terminates a specified sync job").build());
        commands.addOption(Option.builder().longOpt(DELETE_OPT).hasArg().argName("job-id")
                .desc("Deletes a sync job from the server.  The job must be stopped first.  Note that the database is left in-tact").build());
        commands.addOption(Option.builder().longOpt(STATUS_OPT).hasArg().argName("job-id")
                .desc("Queries the server for job status").build());
        commands.addOption(Option.builder().longOpt(SUBMIT_OPT).hasArg().argName("xml-file")
                .desc("Submits a new job to the server").build());
        commands.addOption(Option.builder().longOpt(SET_THREADS_OPT).hasArg().argName("job-id")
                .desc("Sets the number of sync threads on the server.  " +
                        "Requires --" + THREADS_OPT + " argument").build());
        commands.addOption(Option.builder().longOpt(LIST_JOBS_OPT).desc("Lists jobs in the server").build());
        commands.addOption(Option.builder().longOpt(XML_GEN_OPT).hasArg().argName("output-file")
                .desc("Generates a verbose XML config file for the specified plugins").build());
        commands.addOption(Option.builder().longOpt(SET_LOG_LEVEL_OPT).hasArg().argName("log-level").type(Level.class)
                .desc("Sets the logging level of the ecs-sync service").build());
        commands.addOption(Option.builder().longOpt(HOST_INFO_OPT).desc("Gets host information, including ecs-sync version").build());
        commands.setRequired(true);

        opts.addOptionGroup(commands);

        opts.addOption(Option.builder().longOpt(DEBUG_OPT).desc("maximum logging for the ctl client").build());
        opts.addOption(Option.builder().longOpt(VERBOSE_OPT).desc("additional logging for the ctl client").build());

        opts.addOption(Option.builder().longOpt(THREADS_OPT).hasArg().argName("thread-count").desc(
                "Used in conjunction with --" + SET_THREADS_OPT +
                        " to set the number of threads to use for a job.").build());
        opts.addOption(Option.builder().longOpt(LOG_FILE_OPT).hasArg().argName("filename")
                .desc("Filename to write log messages.  Setting to STDOUT or STDERR will write log messages to the " +
                        "appropriate process stream.  Default is STDERR.").build());
        opts.addOption(Option.builder().longOpt(LOG_PATTERN_OPT).hasArg().argName("log4j-pattern").desc("Sets the " +
                "Log4J pattern to use when writing log messages.  Defaults to " +
                LAYOUT_STRING_FILE).build());

        opts.addOption(Option.builder().longOpt(XG_SOURCE_OPT).hasArg().argName("source-prefix")
                .desc("The prefix for the storage plugin to use as the source in the generated config file").build());
        opts.addOption(Option.builder().longOpt(XG_TARGET_OPT).hasArg().argName("target-prefix")
                .desc("The prefix for the storage plugin to use as the target in the generated config file").build());
        opts.addOption(Option.builder().longOpt(XG_FILTERS_OPT).hasArg().argName("filter-list")
                .desc("A comma-delimited list of names of filters to use as the source in the generated config file (optional)").build());
        opts.addOption(Option.builder().longOpt(XG_COMMENTS_OPT).desc("Adds descriptive comments to the generated config file").build());
        opts.addOption(Option.builder().longOpt(XG_SIMPLE_OPT).desc("Does not include advanced options in the generated config file").build());

        opts.addOption(Option.builder().longOpt(ENDPOINT_OPT).hasArg().argName("url")
                .desc("Sets the server endpoint to connect to.  Default is " + DEFAULT_ENDPOINT).build());


        DefaultParser dp = new DefaultParser();

        CommandLine cmd = null;
        try {
            cmd = dp.parse(opts, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            printHelp(opts);
            System.exit(EXIT_ARG_ERROR);
        }

        //
        // Configure Logging
        //
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

        String logFileName = "STDERR";
        if (cmd.hasOption(LOG_FILE_OPT)) {
            logFileName = cmd.getOptionValue(LOG_FILE_OPT);
        }

        // Pattern
        String layoutString = LAYOUT_STRING_FILE;
        if (!System.getProperty("os.name").startsWith("Windows")) {
            if ("STDERR".equals(logFileName) || "STDOUT".equals(logFileName)) {
                layoutString = LAYOUT_STRING_CONSOLE;
            }
        } else {
            // No easy ANSI colors in Windows console :(
            if ("STDERR".equals(logFileName) || "STDOUT".equals(logFileName)) {
                layoutString = LAYOUT_STRING_CONSOLE_NOANSI;
            }
        }
        if (cmd.hasOption(LOG_PATTERN_OPT)) {
            layoutString = cmd.getOptionValue(LOG_PATTERN_OPT);
        }
        LayoutComponentBuilder layout = builder.newLayout("PatternLayout").addAttribute("pattern", layoutString);

        // Appender
        AppenderComponentBuilder appender;
        if (logFileName.equals("STDERR")) {
            appender = builder.newAppender("mainAppender", "Console")
                    .addAttribute("target", ConsoleAppender.Target.SYSTEM_ERR);
        } else if (logFileName.equals("STDOUT")) {
            appender = builder.newAppender("mainAppender", "Console")
                    .addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
        } else {
            // Just a regular file.
            appender = builder.newAppender("mainAppender", "File")
                    .addAttribute("fileName", logFileName);
        }
        appender.add(layout);
        builder.add(appender);

        // Log level
        Level level;
        if (cmd.hasOption(DEBUG_OPT)) {
            level = Level.DEBUG;
        } else if (cmd.hasOption(VERBOSE_OPT)) {
            level = Level.INFO;
        } else {
            level = Level.WARN;
        }
        builder.add(builder.newRootLogger(level).add(builder.newAppenderRef("mainAppender")));

        // Initialize Logging
        try {
            // have to call reconfigure() here because the default config is initialized on JVM startup
            Configurator.reconfigure(builder.build());
        } catch (Exception e) {
            System.err.println("FATAL: Could not configure logging");
            e.printStackTrace();
            System.exit(EXIT_LOGGER_ERROR);
        }

        String endpoint = DEFAULT_ENDPOINT;
        if (cmd.hasOption(ENDPOINT_OPT)) {
            endpoint = cmd.getOptionValue(ENDPOINT_OPT);
        }
        EcsSyncCtl cli = new EcsSyncCtl(endpoint);

        // Execute command
        try {
            if (cmd.hasOption(PAUSE_OPT)) {
                int jobId = Integer.parseInt(cmd.getOptionValue(PAUSE_OPT));
                log.info("Command: Pause Job #{}", jobId);
                cli.pause(jobId);
            } else if (cmd.hasOption(RESUME_OPT)) {
                int jobId = Integer.parseInt(cmd.getOptionValue(RESUME_OPT));
                log.info("Command: Resume Job #{}", jobId);
                cli.resume(jobId);
            } else if (cmd.hasOption(STOP_OPT)) {
                int jobId = Integer.parseInt(cmd.getOptionValue(STOP_OPT));
                log.info("Command: Terminate Job #{}", jobId);
                cli.stop(jobId);
            } else if (cmd.hasOption(DELETE_OPT)) {
                int jobId = Integer.parseInt(cmd.getOptionValue(DELETE_OPT));
                log.info("Command: Delete Job #{}", jobId);
                cli.delete(jobId);
            } else if (cmd.hasOption(STATUS_OPT)) {
                int jobId = Integer.parseInt(cmd.getOptionValue(STATUS_OPT));
                log.info("Command: Status Job #{}", jobId);
                cli.status(jobId);
            } else if (cmd.hasOption(SUBMIT_OPT)) {
                String xmlFile = cmd.getOptionValue(SUBMIT_OPT);
                log.info("Command: Submit file {}", xmlFile);
                cli.submit(xmlFile);
            } else if (cmd.hasOption(SET_THREADS_OPT)) {
                if (!cmd.hasOption(THREADS_OPT)) {
                    System.err.printf("Error: the argument --%s is required for --%s\n", THREADS_OPT, SET_THREADS_OPT);
                    printHelp(opts);
                    System.exit(EXIT_ARG_ERROR);
                }
                int jobId = Integer.parseInt(cmd.getOptionValue(SET_THREADS_OPT));
                Integer threadCount = null;
                if (cmd.hasOption(THREADS_OPT)) {
                    threadCount = Integer.valueOf(cmd.getOptionValue(THREADS_OPT));
                }
                log.info("Command: Set job {} thread count = {}",
                        jobId, threadCount);

                cli.setThreadCount(jobId, threadCount);
            } else if (cmd.hasOption(LIST_JOBS_OPT)) {
                log.info("Command: List Jobs");
                cli.listJobs();
            } else if (cmd.hasOption(XML_GEN_OPT)) {
                log.info("Command: Generate XML");
                if (!cmd.hasOption(XG_SOURCE_OPT) || !cmd.hasOption(XG_TARGET_OPT)) {
                    System.err.printf("Error: the arguments --%s and --%s are required for --%s", XG_SOURCE_OPT, XG_TARGET_OPT, XML_GEN_OPT);
                    printHelp(opts);
                    System.exit(EXIT_ARG_ERROR);
                }
                String outputFile = cmd.getOptionValue(XML_GEN_OPT);
                String source = cmd.getOptionValue(XG_SOURCE_OPT);
                String target = cmd.getOptionValue(XG_TARGET_OPT);
                String filters = cmd.getOptionValue(XG_FILTERS_OPT);
                boolean addComments = cmd.hasOption(XG_COMMENTS_OPT);
                boolean simple = cmd.hasOption(XG_SIMPLE_OPT);
                try {
                    cli.genXml(outputFile, source, target, filters, addComments, !simple);
                } catch (Exception e) {
                    System.err.print("Error: " + e);
                    System.exit(EXIT_UNKNOWN_ERROR);
                }
            } else if (cmd.hasOption(HOST_INFO_OPT)) {
                log.info("Command: Host Info");
                cli.hostInfo();
            } else if (cmd.hasOption(SET_LOG_LEVEL_OPT)) {
                log.info("Command: Set Log Level");
                cli.setLogLevel(LogLevel.valueOf(cmd.getOptionValue(SET_LOG_LEVEL_OPT)));
            } else {
                throw new RuntimeException("Unknown command");
            }
        } catch (ClientHandlerException e) {
            if (e.getCause() instanceof ConnectException) {
                log.error("Could not connect to the ecs-sync service at " + endpoint
                        + " - please make sure the service is running properly");
                log.info("exception log", e);
                System.exit(EXIT_NO_SERVICE);
            } else {
                throw e;
            }
        }

        System.exit(EXIT_SUCCESS);
    }

    private static void printHelp(Options opts) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("java -jar " + JAR_NAME + ".jar", opts, true);
    }

    private void setThreadCount(int jobId, Integer threadCount) {
        JobControl control = new JobControl();
        if (threadCount != null) {
            control.setThreadCount(threadCount);
        }
        controlJob(jobId, control);
    }

    private void resume(int jobId) {
        JobControl control = new JobControl();
        control.setStatus(JobControlStatus.Running);

        controlJob(jobId, control);
    }

    private void pause(int jobId) {
        JobControl control = new JobControl();
        control.setStatus(JobControlStatus.Paused);

        controlJob(jobId, control);
    }

    private void stop(int jobId) {
        JobControl control = new JobControl();
        control.setStatus(JobControlStatus.Stopped);

        controlJob(jobId, control);
    }

    private void controlJob(int jobId, JobControl control) {
        String uri = String.format("%s/job/%d/control", endpoint, jobId);

        ClientResponse response = client.resource(uri).entity(control, "application/xml").post(ClientResponse.class);

        if (response.getStatus() == 404) {
            System.out.printf("No job %d\n", jobId);
            System.exit(EXIT_NO_JOB);
        } else if (response.getStatus() == 200) {
            System.out.println("Command completed successfully");
            System.exit(EXIT_SUCCESS);
        } else {
            System.err.printf("Error controlling job %d: HTTP %d: %s\n", jobId, response.getStatus(),
                    response.getStatusInfo().getReasonPhrase());
            String s = response.getEntity(String.class);
            if (s != null) {
                System.err.println(s);
            }
            System.exit(EXIT_UNKNOWN_ERROR);
        }
    }

    private void status(int jobId) {
        String uri = String.format("%s/job/%d/progress", endpoint, jobId);

        try {
            SyncProgress progress = client.resource(uri).get(SyncProgress.class);

            // when byte *and* object estimates are available, ETA is based on a weighted average of the two percentages
            // with the lesser value counted twice i.e.:
            // ( 2 * min(bytePercent, objectPercent) + max(bytePercent, objectPercent) ) / 3
            double bw = 0, xput = 0, byteRatio = 0, objectRatio = 0, completionRatio = 0;
            long totalBytes = progress.getTotalBytesExpected() - progress.getBytesSkipped();
            long totalObjects = progress.getTotalObjectsExpected() - progress.getObjectsSkipped();
            long etaMs = 0;
            if (progress.getRuntimeMs() > 0) {
                bw = (double) progress.getBytesComplete() * 1000 / progress.getRuntimeMs();
                xput = (double) progress.getObjectsComplete() * 1000 / progress.getRuntimeMs();
                if (totalBytes > 0) {
                    byteRatio = (double) progress.getBytesComplete() / totalBytes;
                    completionRatio = byteRatio;
                }
                if (totalObjects > 0) {
                    objectRatio = (double) progress.getObjectsComplete() / totalObjects;
                    completionRatio = objectRatio;
                }
                if (byteRatio > 0 && objectRatio > 0)
                    completionRatio = (2 * Math.min(byteRatio, objectRatio) + Math.max(byteRatio, objectRatio)) / 3;
                if (completionRatio > 0)
                    etaMs = (long) (progress.getRuntimeMs() / completionRatio - progress.getRuntimeMs());
            }
            String generalError = progress.getRunError() == null ? "" : progress.getRunError();

            System.out.printf("Job Name: %s\n", progress.getJobName());
            System.out.printf("Job Status: %s\n", progress.getStatus());
            System.out.printf("Job Time: %s\n", duration(progress.getRuntimeMs()));
            System.out.printf("Active Query Threads: %d\n", progress.getActiveQueryTasks());
            System.out.printf("Active Sync Threads: %d\n", progress.getActiveSyncTasks());
            System.out.printf("CPU Time: %dms\n", progress.getCpuTimeMs());
            System.out.printf("CPU Usage: %.1f %%\n", progress.getProcessCpuLoad() * 100);
            System.out.printf("Memory Usage: %sB\n", simpleSize(progress.getProcessMemoryUsed()));
            System.out.printf("Objects Expected: %d %s\n", progress.getTotalObjectsExpected(),
                    progress.isEstimatingTotals() ? "(calculating...)" : "");
            System.out.printf("Objects Completed: %d\n", progress.getObjectsComplete());
            System.out.printf("Objects Skipped: %d\n", progress.getObjectsSkipped());
            System.out.printf("Objects Awaiting Retry: %d\n", progress.getObjectsAwaitingRetry());
            System.out.printf("Error Count: %d\n", progress.getObjectsFailed());
            System.out.printf("Bytes Expected: %sB\n", simpleSize(progress.getTotalBytesExpected()));
            System.out.printf("Bytes Completed: %sB\n", simpleSize(progress.getBytesComplete()));
            System.out.printf("Bytes Skipped: %sB\n", simpleSize(progress.getBytesSkipped()));
            System.out.printf("Current BW (source): read: %sB/s write: %sB/s\n",
                    simpleSize(progress.getSourceReadRate()), simpleSize(progress.getSourceWriteRate()));
            System.out.printf("Current BW (target): read: %sB/s write: %sB/s\n",
                    simpleSize(progress.getTargetReadRate()), simpleSize(progress.getTargetWriteRate()));
            System.out.printf("Current Throughput: completed %d/s skipped %d/s failed %d/s\n",
                    progress.getObjectCompleteRate(), progress.getObjectSkipRate(), progress.getObjectErrorRate());
            System.out.printf("Average BW: %sB/s\n", simpleSize((long) bw));
            System.out.printf("Average Throughput: %.1f/s\n", xput);
            System.out.printf("ETA: %s\n", etaMs > 0 ? duration(etaMs) : "N/A");
            System.out.printf("General Error: %s\n", generalError);
        } catch (UniformInterfaceException e) {
            if (e.getResponse().getStatus() == 404) {
                System.out.printf("No job #%d found\n", jobId);
                System.exit(EXIT_NO_JOB);
            } else {
                ClientResponse resp = e.getResponse();
                System.err.printf("Error getting job status: HTTP %d: %s\n", resp.getStatus(), resp.getStatusInfo().getReasonPhrase());
                System.exit(EXIT_UNKNOWN_ERROR);
            }
        }
    }

    private void delete(int jobId) {
        String uri = String.format("%s/job/%d?keepDatabase=true", endpoint, jobId);
        ClientResponse resp = client.resource(uri).delete(ClientResponse.class);

        if (resp.getStatus() == 404) {
            System.out.println("No job running");
            System.exit(EXIT_NO_JOB);
        } else if (resp.getStatus() == 200) {
            System.out.println("Job deleted");
            System.exit(EXIT_SUCCESS);
        } else {
            System.err.printf("Error deleting job: HTTP %d: %s", resp.getStatus(), resp.getStatusInfo().getReasonPhrase());
            System.exit(EXIT_UNKNOWN_ERROR);
        }
    }

    private void submit(String xmlFile) {
        File f = new File(xmlFile);

        if (!f.exists()) {
            System.err.printf("Job file %s does not exist!\n", xmlFile);
            System.exit(EXIT_FILE_NOT_FOUND);
        }

        // if job name not in XML, set to XML file name
        SyncConfig syncConfig;
        try {
            syncConfig = (SyncConfig) pluginResolver.getContext(SyncConfig.class).createUnmarshaller().unmarshal(f);
            if (syncConfig.getJobName() == null) syncConfig.setJobName(f.getName());
        } catch (JAXBException e) {
            throw new RuntimeException("Invalid XML format", e);
        }

        ClientResponse resp = client.resource(endpoint + "/job").entity(syncConfig, "application/xml").put(ClientResponse.class);

        log.debug("HTTP Response {}:{}", resp.getStatus(), resp.getStatusInfo().getReasonPhrase());
        if (resp.getStatus() == 409) {
            System.err.println("Cannot submit job, another job is already running");
            System.exit(EXIT_JOB_CONFLICT);
        } else if (resp.getStatus() > 399) {
            System.err.println("Error submitting job:\n" + resp.getEntity(String.class));
        }

        String job = resp.getHeaders().getFirst("x-emc-job-id");

        System.out.printf("Submitted Job %s\n", job);
    }

    private void listJobs() {
        JobList list = client.resource(endpoint + "/job").accept("application/xml").get(JobList.class);
        System.out.print("JobID  Status        JobName\n");
        System.out.print("-----------------------\n");
        for (JobInfo ji : list.getJobs()) {
            System.out.printf("%5d  %12s  %s\n", ji.getJobId(), ji.getStatus(), ji.getProgress().getJobName());
        }
    }

    private void genXml(String outputFile, String source, String target, String filters, boolean addComments, boolean advancedOptions)
            throws Exception {
        if (!source.endsWith(":")) source += ":";
        if (!target.endsWith(":")) target += ":";
        String[] filterA = (filters == null) ? new String[0] : filters.split(",");
        String xml = XmlGenerator.generateXml(addComments, advancedOptions, source, target, filterA);
        try (OutputStream outputStream = new FileOutputStream(outputFile)) {
            outputStream.write(xml.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void hostInfo() {
        HostInfo info = client.resource(endpoint + "/host").accept("application/xml").get(HostInfo.class);
        System.out.println("Host Info");
        System.out.println("-----------------------");
        System.out.println("ECS-Sync version: " + info.getEcsSyncVersion());
        System.out.println("CPU Count: " + info.getHostCpuCount());
        System.out.println("CPU Load: " + info.getHostCpuLoad());
        System.out.println("Total Memory: " + simpleSize(info.getHostTotalMemory()));
        System.out.println("Used Memory: " + simpleSize(info.getHostMemoryUsed()));
        System.out.println("Log Level: " + info.getLogLevel());
    }

    private void setLogLevel(LogLevel logLevel) {
        ClientResponse response = client.resource(endpoint + "/host/logging?level=" + logLevel).accept("application/xml").post(ClientResponse.class);

        if (response.getStatus() == 200) {
            System.out.println("Command completed successfully");
            System.exit(EXIT_SUCCESS);
        } else {
            System.err.printf("Error setting log level: HTTP %d: %s\n", response.getStatus(),
                    response.getStatusInfo().getReasonPhrase());
            String s = response.getEntity(String.class);
            if (s != null) {
                System.err.println(s);
            }
            System.exit(EXIT_UNKNOWN_ERROR);
        }
    }

    private String duration(long millis) {
        long secs = (millis / 1000) % 60;
        long mins = (millis / 60000) % 60;
        long hours = millis / 3600000;
        String duration = String.format("%dms", millis % 1000);
        if (secs > 0) duration = String.format("%ds ", secs) + duration;
        if (mins > 0) duration = String.format("%dm ", mins) + duration;
        if (hours > 0) duration = String.format("%dh ", hours) + duration;
        return duration;
    }

    private String simpleSize(long size) {
        if (size <= 0) return "" + size;
        long base = 1024L;
        int decimals = 1;
        List<String> prefix = Arrays.asList("", "K", "M", "G", "T");
        int i = (int) (Math.log(size) / Math.log(base));
        i = (i >= prefix.size() ? prefix.size() - 1 : i);
        double value = Math.round((size / Math.pow(base, i)) * Math.pow(10, decimals)) / Math.pow(10, decimals);
        return i == 0 ? "" + size : String.format("%.1f%s", value, prefix.get(i));
    }

    private final String endpoint;
    private final PluginResolver pluginResolver;
    private final Client client;

    public EcsSyncCtl(String endpoint) {
        this.endpoint = endpoint;
        this.pluginResolver = new PluginResolver();

        // initialize REST client
        ClientConfig cc = new DefaultClientConfig();
        cc.getSingletons().add(pluginResolver);
        this.client = Client.create(cc);
    }
}
