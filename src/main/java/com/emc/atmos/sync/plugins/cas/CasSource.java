/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
package com.emc.atmos.sync.plugins.cas;

import com.emc.atmos.sync.AtmosSync2;
import com.emc.atmos.sync.plugins.CommonOptions;
import com.emc.atmos.sync.plugins.MultithreadedCrawlSource;
import com.emc.atmos.sync.plugins.SyncPlugin;
import com.filepool.fplibrary.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.text.MessageFormat;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO: make this compatible with any destination
 * TODO: more logging
 */
public class CasSource extends MultithreadedCrawlSource implements InitializingBean {
    private static final Logger l4j = Logger.getLogger(CasSource.class);

    public static final String SOURCE_CLIP_LIST_OPTION = "source-clip-list";
    public static final String SOURCE_CLIP_LIST_DESC = "The file containing the list of clip IDs to copy (newline separated). Use - to read the list from standard input.";
    public static final String SOURCE_CLIP_LIST_ARG_NAME = "filename";

    protected static final int DEFAULT_BUFFER_SIZE = 1048576; // 1MB

    protected static final String APPLICATION_NAME = AtmosSync2.class.getSimpleName();
    protected static final String APPLICATION_VERSION = AtmosSync2.class.getPackage().getImplementationVersion();

    protected String connectionString;
    protected String clipIdFile;
    protected int bufferSize = DEFAULT_BUFFER_SIZE;
    protected FPPool pool;
    protected String lastResultCreateTime;

    @Override
    public void run() {
        try {
            running = true;

            FPPool.RegisterApplication(APPLICATION_NAME, APPLICATION_VERSION);

            // Check connection
            pool = new FPPool(connectionString);
            FPPool.PoolInfo info = pool.getPoolInfo();
            LogMF.info(l4j, "Connected to source: {0} ({1}) using CAS v.{2}",
                    info.getClusterName(), info.getClusterID(), info.getVersion());

            // verify we have appropriate privileges
            if (pool.getCapability(FPLibraryConstants.FP_READ, FPLibraryConstants.FP_ALLOWED).equals("False"))
                throw new IllegalArgumentException("READ is not supported for this pool connection");

            if (clipIdFile != null)
                // read clip IDs from file
                readFile();
            else {
                // query for all clips
                queryAllClips();
            }

        } catch (Exception e) {
            l4j.error("unrecoverable error", e);
            if (lastResultCreateTime != null)
                l4j.error("last query result create-date: " + lastResultCreateTime);
        } finally {
            if (pool != null) try {
                pool.Close();
            } catch (Throwable t) {
                l4j.warn("could not close pool", t);
            }
        }
    }

    @Override
    public void terminate() {
        running = false;
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        Options opts = new Options();

        opts.addOption(OptionBuilder.withDescription(SOURCE_CLIP_LIST_DESC)
                .withLongOpt(SOURCE_CLIP_LIST_OPTION)
                .hasArg().withArgName(SOURCE_CLIP_LIST_ARG_NAME).create());

        return opts;
    }

    @Override
    public boolean parseOptions(CommandLine line) {
        if (line.hasOption(CommonOptions.SOURCE_OPTION)) {
            Pattern p = Pattern.compile(CasUtil.URI_PATTERN);
            String source = line.getOptionValue(CommonOptions.SOURCE_OPTION);
            Matcher m = p.matcher(source);
            if (!m.matches()) {
                LogMF.debug(l4j, "{0} does not match {1}", source, p);
                return false;
            }

            connectionString = source.replaceFirst("^cas://", "");

            if (line.hasOption(SOURCE_CLIP_LIST_OPTION)) {
                clipIdFile = line.getOptionValue(SOURCE_CLIP_LIST_OPTION);
                if (!"-".equals(clipIdFile)) {
                    // Verify file
                    File f = new File(clipIdFile);
                    if (!f.exists()) {
                        throw new IllegalArgumentException(
                                MessageFormat.format("The clip list file {0} does not exist", clipIdFile));
                    }
                }
            }

            if (line.hasOption(CommonOptions.IO_BUFFER_SIZE_OPTION)) {
                bufferSize = Integer.parseInt(line.getOptionValue(CommonOptions.IO_BUFFER_SIZE_OPTION));
            }

            // Parse threading options
            super.parseOptions(line);

            return true;
        }

        return false;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.hasText(connectionString);
    }

    @Override
    public void validateChain(SyncPlugin first) {
        SyncPlugin next = first.getNext();
        if (next.getClass().getSimpleName().equals("CuaFilesystemDestination")) return;
        if (!(next instanceof CasDestination))
            throw new UnsupportedOperationException("CasSource is currently only compatible with CasDestination");
    }

    @Override
    public String getName() {
        return "CAS Source";
    }

    @Override
    public String getDocumentation() {
        return "The CAS source plugin is triggered by the source pattern:\n" +
                "cas://host[:port][,host[:port]...]?name=<name>,secret=<secret>\n" +
                "or cas://host[:port][,host[:port]...]?<pea_file>\n" +
                "Note that <name> should be of the format <subtenant_id>:<uid>. " +
                "This is passed to the CAS SDK as the connection string " +
                "(you can use primary=, secondary=, etc. in the server hints). " +
                "This source plugin is multithreaded and you should use the " +
                "--source-threads option to specify how many threads to use. " +
                "The default thread count is one. " +
                "Note that the CAS SDK handles streams differently and reads and " +
                "writes are done in parallel, effectively doubling the thread " +
                "count. The buffer is also handled differently and the default " +
                "buffer size is increased to 1MB to compensate.";
    }

    protected void readFile() throws Exception {
        initQueue();

        BufferedReader reader = null;
        try {
            if ("-".equals(clipIdFile)) {
                reader = new BufferedReader(new InputStreamReader(System.in));
            } else {
                reader = new BufferedReader(new FileReader(clipIdFile));
            }

            String clipId;
            while (running && (clipId = reader.readLine()) != null) {
                l4j.debug("submitting ReadClipTask for " + clipId);
                submitTransferTask(new ReadClipTask(this, clipId, bufferSize));
            }
        } finally {
            try {
                if (reader != null) reader.close();
            } catch (Throwable t) {
                l4j.warn("could not close file", t);
            }
        }

        // wait for transfer tasks to complete
        runQueue();
    }

    protected void queryAllClips() throws Exception {
        initQueue();

        // verify we have appropriate privileges
        if (pool.getCapability(FPLibraryConstants.FP_CLIPENUMERATION, FPLibraryConstants.FP_ALLOWED).equals("False"))
            throw new IllegalArgumentException("QUERY is not supported for this pool connection.");

        FPQueryExpression query = new FPQueryExpression();
        query.setStartTime(0);
        query.setEndTime(-1);
        query.setType(FPLibraryConstants.FP_QUERY_TYPE_EXISTING);
        query.selectField("creation.date");

        final FPPoolQuery poolQuery = new FPPoolQuery(pool, query);

        try {
            FPQueryResult queryResult;
            boolean searching = true;
            while (searching && running) {
                queryResult = time(new Callable<FPQueryResult>() {
                    @Override
                    public FPQueryResult call() throws Exception {
                        return poolQuery.FetchResult();
                    }
                }, CasUtil.OPERATION_FETCH_QUERY_RESULT);
                try {
                    switch (queryResult.getResultCode()) {
                        case FPLibraryConstants.FP_QUERY_RESULT_CODE_OK:
                            l4j.debug("query result OK; creating ReadClipTask.");
                            lastResultCreateTime = queryResult.getField("creation.date");
                            submitTransferTask(new ReadClipTask(this, queryResult.getClipID(), bufferSize));
                            break;

                        case FPLibraryConstants.FP_QUERY_RESULT_CODE_INCOMPLETE:
                            l4j.info("received FP_QUERY_RESULT_CODE_INCOMPLETE error, invalid C-Clip, trying again.");
                            break;

                        case FPLibraryConstants.FP_QUERY_RESULT_CODE_COMPLETE:
                            l4j.info("received FP_QUERY_RESULT_CODE_COMPLETE, there should have been a previous "
                                    + "FP_QUERY_RESULT_CODE_INCOMPLETE error reported.");
                            break;

                        case FPLibraryConstants.FP_QUERY_RESULT_CODE_PROGRESS:
                            l4j.info("received FP_QUERY_RESULT_CODE_PROGRESS, continuing.");
                            break;

                        case FPLibraryConstants.FP_QUERY_RESULT_CODE_ERROR:
                            l4j.info("received FP_QUERY_RESULT_CODE_ERROR error, retrying again");
                            break;

                        case FPLibraryConstants.FP_QUERY_RESULT_CODE_END:
                            l4j.warn("end of query reached, exiting.");
                            searching = false;
                            break;

                        case FPLibraryConstants.FP_QUERY_RESULT_CODE_ABORT:
                            // query aborted due to server side issue or start time
                            // is later than server time.
                            throw new RuntimeException("received FP_QUERY_RESULT_CODE_ABORT error, exiting.");

                        default:
                            throw new RuntimeException("received error: " + queryResult.getResultCode());
                    }
                } finally {
                    queryResult.Close();
                }
            } //while
        } finally {
            poolQuery.Close();
        }

        // wait for transfer tasks to complete
        runQueue();
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public String getClipIdFile() {
        return clipIdFile;
    }

    public void setClipIdFile(String clipIdFile) {
        this.clipIdFile = clipIdFile;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
