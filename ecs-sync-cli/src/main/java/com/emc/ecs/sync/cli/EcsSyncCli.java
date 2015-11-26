package com.emc.ecs.sync.cli;

import com.emc.ecs.sync.bean.*;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.apache.commons.cli.*;
import org.apache.log4j.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Entry point class for the ECS Sync CLI
 */
public class EcsSyncCli {
    private static final Logger l4j = Logger.getLogger(EcsSyncCli.class);
    private static final String PAUSE_OPT = "pause";
    private static final String RESUME_OPT = "resume";
    private static final String STOP_OPT = "stop";
    private static final String DELETE_OPT = "delete";
    private static final String STATUS_OPT = "status";
    private static final String SUBMIT_OPT = "submit";
    private static final String SET_THREADS_OPT = "set-threads";
    private static final String SYNC_THREADS_OPT = "sync-threads";
    private static final String QUERY_THREADS_OPT = "query-threads";
    private static final String LOG_LEVEL_OPT = "log-level";
    private static final String LOG_FILE_OPT = "log-file";
    private static final String LOG_PATTERN_OPT = "log-pattern";
    private static final String LIST_JOBS_OPT = "list-jobs";
    private static final String ENDPOINT_OPT = "endpoint";

    private static final String DEFAULT_ENDPOINT = "http://localhost:9200";
    private static final String JAR_NAME = "ecs-sync-{version}";

    private static final String LAYOUT_STRING_FILE = "%d{yyyy-MM-dd HH:mm:ss} %-5p [%t] %c{1}:%L - %m%n";
    private static final String LAYOUT_STRING_CONSOLE = "%d{MM-dd HH:mm:ss}%-5p [%t] %c{1}:%L - %m%n";


    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_ARG_ERROR = 255;
    private static final int EXIT_FILE_NOT_FOUND = 2;
    private static final int EXIT_LOGGER_ERROR = 3;
    private static final int EXIT_JOB_CONFLICT = 4;
    private static final int EXIT_NO_JOB = 5;
    private static final int EXIT_UNKNOWN_ERROR=99;


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
                .desc("Deletes a sync job from the server.  The job must be stopped first.").build());
        commands.addOption(Option.builder().longOpt(STATUS_OPT).hasArg().argName("job-id")
                .desc("Queries the server for job status").build());
        commands.addOption(Option.builder().longOpt(SUBMIT_OPT).hasArg().argName("xml-file")
                .desc("Submits a new job to the server").build());
        commands.addOption(Option.builder().longOpt(SET_THREADS_OPT).hasArg().argName("job-id")
                .desc("Sets the number of sync threads on the server.  " +
                        "Requires --" + QUERY_THREADS_OPT + " and/or --" + SYNC_THREADS_OPT + " arguments").build());
        commands.addOption(Option.builder().longOpt(LIST_JOBS_OPT).desc("Lists jobs in the server").build());
        commands.setRequired(true);

        opts.addOptionGroup(commands);

        opts.addOption(Option.builder().longOpt(SYNC_THREADS_OPT).hasArg().argName("thread-count").desc(
                "Used in conjunction with --" + SET_THREADS_OPT +
                        " to set the number of sync threads to use for a job.").build());
        opts.addOption(Option.builder().longOpt(QUERY_THREADS_OPT).hasArg().argName("thread-count").desc(
                "Used in conjunction with --" + SET_THREADS_OPT +
                        " to set the number of query threads to use for a job.").build());
        opts.addOption(Option.builder().longOpt(LOG_LEVEL_OPT).hasArg().argName("level").type(Level.class)
                .desc("Sets the log level: DEBUG, INFO, WARN, ERROR, or FATAL.  Default is ERROR.").build());
        opts.addOption(Option.builder().longOpt(LOG_FILE_OPT).hasArg().argName("filename")
                .desc("Filename to write log messages.  Setting to STDOUT or STDERR will write log messages to the " +
                        "appropriate process stream.  Default is STDERR.").build());
        opts.addOption(Option.builder().longOpt(LOG_PATTERN_OPT).hasArg().argName("log4j-pattern").desc("Sets the " +
                "Log4J pattern to use when writing log messages.  Defaults to " +
                LAYOUT_STRING_FILE).build());

        opts.addOption(Option.builder().longOpt(ENDPOINT_OPT).hasArg().argName("url")
                .desc("Sets the server endpoint to connect to.  Default is " + DEFAULT_ENDPOINT).build());


        DefaultParser dp = new DefaultParser();

        CommandLine cmd = null;
        try {
            cmd = dp.parse(opts, args);
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("java -jar " + JAR_NAME +".jar", opts, true);
            System.exit(EXIT_ARG_ERROR);
        }

        //
        // Configure Logging
        //
        String logFileName = "STDERR";
        if(cmd.hasOption(LOG_FILE_OPT)) {
            logFileName = cmd.getOptionValue(LOG_FILE_OPT);
        }

        // Pattern
        String layoutString = LAYOUT_STRING_FILE;
        if("STDERR".equals(logFileName) || "STDOUT".equals(logFileName)) {
            layoutString = LAYOUT_STRING_CONSOLE;
        }
        if(cmd.hasOption(LOG_PATTERN_OPT)) {
            layoutString = cmd.getOptionValue(LOG_PATTERN_OPT);
        }
        PatternLayout layout  = new PatternLayout(layoutString);

        // Appender
        Appender appender = null;
        if(logFileName.equals("STDERR")) {
            appender = new ConsoleAppender(layout, "System.err");
        } else if(logFileName.equals("STDOUT")) {
            appender = new ConsoleAppender(layout, "System.out");
        } else {
            // Just a regular file.
            try {
                appender = new FileAppender(layout, logFileName);
            } catch (IOException e) {
                System.err.println("FATAL: Could not configure appender");
                e.printStackTrace();
                System.exit(EXIT_LOGGER_ERROR);
            }
        }
        LogManager.getRootLogger().addAppender(appender);

        // Log level
        String logLevel = "ERROR";
        if(cmd.hasOption(LOG_LEVEL_OPT)) {
            logLevel = cmd.getOptionValue(LOG_LEVEL_OPT);
        }
        LogManager.getRootLogger().setLevel(Level.toLevel(logLevel));

        String endpoint = DEFAULT_ENDPOINT;
        if(cmd.hasOption(ENDPOINT_OPT)) {
            endpoint = cmd.getOptionValue(ENDPOINT_OPT);
        }
        EcsSyncCli cli = new EcsSyncCli(endpoint, null, null);


        if(cmd.hasOption(PAUSE_OPT)) {
            int jobId = Integer.parseInt(cmd.getOptionValue(PAUSE_OPT));
            LogMF.info(l4j, "Command: Pause Job #{0}", jobId);
            cli.pause(jobId);
        } else if(cmd.hasOption(RESUME_OPT)) {
            int jobId = Integer.parseInt(cmd.getOptionValue(RESUME_OPT));
            LogMF.info(l4j, "Command: Resume Job #{0}", jobId);
            cli.resume(jobId);
        } else if (cmd.hasOption(STOP_OPT)) {
            int jobId = Integer.parseInt(cmd.getOptionValue(STOP_OPT));
            LogMF.info(l4j, "Command: Terminate Job #{0}", jobId);
            cli.stop(jobId);
        } else if(cmd.hasOption(DELETE_OPT)) {
            int jobId = Integer.parseInt(cmd.getOptionValue(DELETE_OPT));
            LogMF.info(l4j, "Command: Delete Job #{0}", jobId);
            cli.delete(jobId);
        } else if(cmd.hasOption(STATUS_OPT)) {
            int jobId = Integer.parseInt(cmd.getOptionValue(STATUS_OPT));
            LogMF.info(l4j, "Command: Status Job #{0}", jobId);
            cli.status(jobId);
        } else if(cmd.hasOption(SUBMIT_OPT)) {
            String xmlFile = cmd.getOptionValue(SUBMIT_OPT);
            LogMF.info(l4j, "Command: Submit file {0}", xmlFile);
            cli.submit(xmlFile);
        } else if(cmd.hasOption(SET_THREADS_OPT)) {
            if(!(cmd.hasOption(SYNC_THREADS_OPT) || cmd.hasOption(QUERY_THREADS_OPT))) {
                System.err.printf("Error: the argument --%s and/or --%s is required for --%s\n", SYNC_THREADS_OPT,
                        QUERY_THREADS_OPT, SET_THREADS_OPT);
                HelpFormatter hf = new HelpFormatter();
                hf.printHelp("java -jar " + JAR_NAME +".jar", opts, true);
                System.exit(EXIT_ARG_ERROR);
            }
            int jobId = Integer.parseInt(cmd.getOptionValue(SET_THREADS_OPT));
            Integer syncThreadCount = null;
            Integer queryThreadCount = null;
            if(cmd.hasOption(SYNC_THREADS_OPT)) {
                syncThreadCount = new Integer(cmd.getOptionValue(SYNC_THREADS_OPT));
            }
            if(cmd.hasOption(QUERY_THREADS_OPT)) {
                queryThreadCount = new Integer(cmd.getOptionValue(QUERY_THREADS_OPT));
            }
            LogMF.info(l4j, "Command: Set job {0} sync thread count = {1}, query thread count = {2}",
                    jobId, syncThreadCount, queryThreadCount);

            cli.setThreadCount(jobId, syncThreadCount, queryThreadCount);
        } else if(cmd.hasOption(LIST_JOBS_OPT)) {
            l4j.info("Command: List Jobs");
            cli.listJobs();
        } else {
            throw new RuntimeException("Unknown command");
        }

        System.exit(EXIT_SUCCESS);
    }

    private void setThreadCount(int jobId, Integer syncThreadCount, Integer queryThreadCount) {
        JobControl control = new JobControl();
        if(syncThreadCount != null) {
            control.setSyncThreadCount(syncThreadCount);
        }
        if(queryThreadCount != null) {
            control.setQueryThreadCount(queryThreadCount);
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
        Client client = new Client();
        String uri = String.format("%s/job/%d/control", endpoint, jobId);

        ClientResponse response = client.resource(uri).entity(control, "application/xml").post(ClientResponse.class);

        if(response.getStatus() == 404) {
            System.out.printf("No job %d\n", jobId);
            System.exit(EXIT_NO_JOB);
        } else if(response.getStatus() == 200) {
            System.out.println("Command completed successfully");
            System.exit(EXIT_SUCCESS);
        } else {
            System.err.printf("Error controlling job %d: HTTP %d: %s\n", jobId, response.getStatus(),
                    response.getStatusInfo().getReasonPhrase());
            String s = response.getEntity(String.class);
            if(s != null) {
                System.err.println(s);
            }
            System.exit(EXIT_UNKNOWN_ERROR);
        }
    }

    private void status(int jobId) {
        Client client = new Client();
        String uri = String.format("%s/job/%d/progress", endpoint, jobId);

        try {
            SyncProgress progress = client.resource(uri).get(SyncProgress.class);

            // when byte *and* object estimates are available, ETA is based on a weighted average of the two percentages
            // with the lesser value counted twice i.e.:
            // ( 2 * min(bytePercent, objectPercent) + max(bytePercent, objectPercent) ) / 3
            double bw = 0, xput = 0, byteRatio = 0, objectRatio = 0, completionRatio = 0;
            long etaMs = 0;
            if (progress.getRuntimeMs() > 0) {
                bw = (double) progress.getBytesComplete() * 1000 / progress.getRuntimeMs();
                xput = (double) progress.getObjectsComplete() * 1000 / progress.getRuntimeMs();
                if (progress.getTotalBytesExpected() > 0) {
                    byteRatio = (double) progress.getBytesComplete() / progress.getTotalBytesExpected();
                    completionRatio = byteRatio;
                }
                if (progress.getTotalObjectsExpected() > 0) {
                    objectRatio = (double) progress.getObjectsComplete() / progress.getTotalObjectsExpected();
                    completionRatio = objectRatio;
                }
                if (byteRatio > 0 && objectRatio > 0)
                    completionRatio = (2 * Math.min(byteRatio, objectRatio) + Math.max(byteRatio, objectRatio)) / 3;
                if (completionRatio > 0)
                    etaMs = (long) (progress.getRuntimeMs() / completionRatio - progress.getRuntimeMs());
            }

            System.out.printf("Job Time: %s\n", duration(progress.getRuntimeMs()));
            System.out.printf("Active Query Threads: %d\n", progress.getActiveQueryTasks());
            System.out.printf("Active Sync Threads: %d\n", progress.getActiveSyncTasks());
            System.out.printf("CPU Time: %dms\n", progress.getCpuTimeMs());
            System.out.printf("CPU Usage: %.1f %%\n", progress.getProcessCpuLoad() * 100);
            System.out.printf("Memory Usage: %sB\n", simpleSize(progress.getProcessMemoryUsed()));
            System.out.printf("Objects Completed: %d\n", progress.getObjectsComplete());
            System.out.printf("Objects Expected: %s\n", progress.getTotalObjectsExpected());
            System.out.printf("Error Count: %d\n", progress.getObjectsFailed());
            System.out.printf("Bytes Completed: %sB\n", simpleSize(progress.getBytesComplete()));
            System.out.printf("Bytes Expected: %sB\n", simpleSize(progress.getTotalBytesExpected()));
            System.out.printf("Current BW (source): read: %sB/s write: %sB/s\n",
                    simpleSize(progress.getSourceReadRate()), simpleSize(progress.getSourceWriteRate()));
            System.out.printf("Current BW (target): read: %sB/s write: %sB/s\n",
                    simpleSize(progress.getTargetReadRate()), simpleSize(progress.getTargetWriteRate()));
            System.out.printf("Current Throughput: completed %d/s failed %d/s\n",
                    progress.getObjectCompleteRate(), progress.getObjectErrorRate());
            System.out.printf("Average BW: %sB/s\n", simpleSize((long) bw));
            System.out.printf("Average Throughput: %.1f/s\n", xput);
            System.out.printf("ETA: %s\n", etaMs > 0 ? duration(etaMs) : "N/A");
        } catch(UniformInterfaceException e) {
            if(e.getResponse().getStatus() == 404) {
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
        Client client = new Client();
        String uri = String.format("%s/job/%d", endpoint, jobId);
        ClientResponse resp = client.resource(uri).delete(ClientResponse.class);

        if(resp.getStatus() == 404) {
            System.out.println("No job running");
            System.exit(EXIT_NO_JOB);
        } else if(resp.getStatus() == 200) {
            System.out.println("Job deleted");
            System.exit(EXIT_SUCCESS);
        } else {
            System.err.printf("Error deleting job: HTTP %d: %s", resp.getStatus(), resp.getStatusInfo().getReasonPhrase());
            System.exit(EXIT_UNKNOWN_ERROR);
        }
    }

    private void submit(String xmlFile) {
        File f = new File(xmlFile);

        if(!f.exists()) {
            System.err.printf("Job file %s does not exist!\n", xmlFile);
            System.exit(EXIT_FILE_NOT_FOUND);
        }

        Client client = new Client();
        ClientResponse resp = client.resource(endpoint + "/job").entity(f, "application/xml").put(ClientResponse.class);

        LogMF.debug(l4j, "HTTP Response {0}:{1}", resp.getStatus(), resp.getStatusInfo().getReasonPhrase());
        if(resp.getStatus() == 409) {
            System.err.println("Cannot submit job, another job is already running");
            System.exit(EXIT_JOB_CONFLICT);
        }

        String job = resp.getHeaders().getFirst("x-emc-job-id");

        System.out.printf("Submitted Job %s\n", job);
    }

    private void listJobs() {
        Client client = new Client();
        JobList list = client.resource(endpoint + "/job").accept("application/xml").get(JobList.class);
        System.out.printf("JobID  Status\n");
        System.out.printf("-----------------------\n");
        for(JobInfo ji : list.getJobs()) {
            System.out.printf("%5d  %s\n", ji.getJobId(), ji.getStatus());
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
        List prefix = Arrays.asList("", 'K', 'M', 'G', 'T');
        int i = (int) (Math.log(size) / Math.log(base));
        i = (i >= prefix.size() ? prefix.size() - 1 : i);
        double value = Math.round((size / Math.pow(base, i)) * Math.pow(10, decimals)) / Math.pow(10, decimals);
        return i == 0 ? "" + size : String.format("%.1f%s", value, prefix.get(i));
    }

    private String endpoint;
    private String user;
    private String pass;

    public EcsSyncCli(String endpoint, String user, String pass) {
        this.endpoint = endpoint;
        this.user = user;
        this.pass = pass;
    }
}
