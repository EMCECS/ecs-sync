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
package com.emc.vipr.sync.source;

import com.emc.vipr.sync.ViPRSync;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.ClipSyncObject;
import com.emc.vipr.sync.model.object.S3SyncObject;
import com.emc.vipr.sync.target.CasTarget;
import com.emc.vipr.sync.target.CuaFilesystemTarget;
import com.emc.vipr.sync.target.DeleteSourceTarget;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.*;
import com.filepool.fplibrary.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO: make this compatible with any target
 */
public class CasSource extends SyncSource<ClipSyncObject> {
    private static final Logger l4j = Logger.getLogger(CasSource.class);

    public static final String SOURCE_CLIP_LIST_OPTION = "source-clip-list";
    public static final String SOURCE_CLIP_LIST_DESC = "The file containing the list of clip IDs to copy (newline separated). Use - to read the list from standard input.";
    public static final String SOURCE_CLIP_LIST_ARG_NAME = "filename";

    public static final String SOURCE_DELETE_REASON_OPTION = "source-delete-reason";
    public static final String SOURCE_DELETE_REASON_DESC = "When deleting source clips, this is the audit string";
    public static final String SOURCE_DELETE_REASON_ARG_NAME = "audit-string";

    protected static final int DEFAULT_BUFFER_SIZE = 1048576; // 1MB
    protected static final String DEFAULT_DELETE_REASON = "Deleted by ViPRSync";

    protected static final String APPLICATION_NAME = CasSource.class.getName();
    protected static final String APPLICATION_VERSION = ViPRSync.class.getPackage().getImplementationVersion();

    protected String connectionString;
    protected String clipIdFile;
    protected FPPool pool;
    protected String lastResultCreateTime;
    protected String deleteReason = DEFAULT_DELETE_REASON;

    public CasSource() {
        bufferSize = DEFAULT_BUFFER_SIZE;
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.matches(CasUtil.URI_PATTERN);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(SOURCE_CLIP_LIST_OPTION).withDescription(SOURCE_CLIP_LIST_DESC)
                .hasArg().withArgName(SOURCE_CLIP_LIST_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(SOURCE_DELETE_REASON_OPTION).withDescription(SOURCE_DELETE_REASON_DESC)
                .hasArg().withArgName(SOURCE_DELETE_REASON_ARG_NAME).create());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        Pattern p = Pattern.compile(CasUtil.URI_PATTERN);
        Matcher m = p.matcher(sourceUri);
        if (!m.matches())
            throw new ConfigurationException(String.format("%s does not match %s", sourceUri, p));

        connectionString = sourceUri.replaceFirst("^" + CasUtil.URI_PREFIX, "");

        if (line.hasOption(SOURCE_CLIP_LIST_OPTION))
            clipIdFile = line.getOptionValue(SOURCE_CLIP_LIST_OPTION);

        if (line.hasOption(SOURCE_DELETE_REASON_OPTION))
            deleteReason = line.getOptionValue(SOURCE_DELETE_REASON_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (!(target instanceof CuaFilesystemTarget) && !(target instanceof CasTarget)
                && !(target instanceof DeleteSourceTarget))
            throw new ConfigurationException("CasSource is currently only compatible with CasTarget, CuaFilesystemTarget or DeleteSourceTarget");

        Assert.hasText(connectionString);

        if (clipIdFile != null && !"-".equals(clipIdFile)) {
            // Verify file
            File f = new File(clipIdFile);
            if (!f.exists())
                throw new ConfigurationException(String.format("The clip list file %s does not exist", clipIdFile));
        }

        try {
            if (pool == null) {
                FPPool.RegisterApplication(APPLICATION_NAME, APPLICATION_VERSION);
                pool = new FPPool(connectionString);
            }

            // Check connection
            FPPool.PoolInfo info = pool.getPoolInfo();
            LogMF.info(l4j, "Connected to source: {0} ({1}) using CAS v.{2}",
                    info.getClusterName(), info.getClusterID(), info.getVersion());

            // verify we have appropriate privileges
            if (pool.getCapability(FPLibraryConstants.FP_READ, FPLibraryConstants.FP_ALLOWED).equals("False"))
                throw new ConfigurationException("READ is not supported for this pool connection");

            if (target instanceof DeleteSourceTarget
                    && pool.getCapability(FPLibraryConstants.FP_DELETE, FPLibraryConstants.FP_ALLOWED).equals("False"))
                throw new ConfigurationException("DELETE is not supported for this pool connection");

        } catch (FPLibraryException e) {
            throw new RuntimeException("error creating pool", e);
        }
    }

    @Override
    public Iterator<ClipSyncObject> iterator() {
        try {
            if (clipIdFile != null)
                // read clip IDs from file
                return clipListIterator();
            else {
                // query for all clips
                return queryIterator();
            }
        } catch (FPLibraryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sync(final ClipSyncObject syncObject, SyncFilter filterChain) {
        int tagCount = 0;
        FPClip clip = null;
        FPTag tag = null;
        List<ClipTag> tags = new ArrayList<ClipTag>();
        try {
            // the entire clip (and all blobs) will be sent at once, so we can keep references to clips and tags open.
            // open the clip
            clip = CasUtil.openClip(this, pool, syncObject.getRawSourceIdentifier());

            CasUtil.hydrateClipData(this, syncObject, clip);

            // pull all clip tags
            while ((tag = clip.FetchNext()) != null) {
                tags.add(new ClipTag(tag, tagCount++, bufferSize));
            }
            syncObject.setTags(tags);

            // sync the object
            filterChain.filter(syncObject);

        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException(e);
        } finally {
            // close current tag ref
            try {
                if (tag != null) tag.Close();
            } catch (Throwable t) {
                l4j.warn("could not close tag " + syncObject.getRawSourceIdentifier() + "." + tagCount + ": " + t.getMessage());
            }
            // close blob tags
            for (ClipTag blobSync : tags) {
                try {
                    blobSync.getTag().Close();
                } catch (Throwable t) {
                    l4j.warn("could not close tag " + syncObject.getRawSourceIdentifier() + "." + blobSync.getTagNum()
                            + ": " + t.getMessage());
                }
            }
            // close clip
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                l4j.warn("could not close clip " + syncObject.getRawSourceIdentifier() + ": " + t.getMessage());
            }
        }
    }

    @Override
    public Iterator<ClipSyncObject> childIterator(ClipSyncObject syncObject) {
        return null;
    }

    @Override
    public void delete(ClipSyncObject syncObject) {
        try {
            FPClip.AuditedDelete(pool, syncObject.getRawSourceIdentifier(), deleteReason,
                    FPLibraryConstants.FP_OPTION_DEFAULT_OPTIONS);
        } catch (FPLibraryException e) {
            throw new RuntimeException(e);
        }
    }

//    @Override
//    public void delete(S3SyncObject syncObject) {
//
//    }

    @Override
    public void verify(ClipSyncObject syncObject, SyncFilter filterChain) {
        FPClip clip = null;
        try {
            // make sure our source object has its CDF content (it won't if this is verify-only)
            if (syncObject.getCdfData() == null) {
                clip = CasUtil.openClip(this, pool, syncObject.getRawSourceIdentifier());
                CasUtil.hydrateClipData(this, syncObject, clip);
            }
            super.verify(syncObject, filterChain);
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException(e);
        } finally {
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                l4j.warn("could not close clip " + syncObject.getRawSourceIdentifier() + ": " + t.getMessage());
            }
        }
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
                "Note that <name> should be of the format <subtenant_id>:<uid> " +
                "when connecting to an Atmos system. " +
                "This is passed to the CAS SDK as the connection string " +
                "(you can use primary=, secondary=, etc. in the server hints). " +
                "Note that the CAS SDK handles streams differently and reads and " +
                "writes are done in parallel, effectively doubling the thread " +
                "count. The buffer is also handled differently and the default " +
                "buffer size is increased to 1MB to compensate.";
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (pool != null) try {
            pool.Close();
        } catch (Throwable t) {
            l4j.warn("could not close pool: " + t.getMessage());
        }
        pool = null;
    }

    protected Iterator<ClipSyncObject> clipListIterator() throws FPLibraryException {
        final Iterator<String> fileIterator = new FileLineIterator(clipIdFile);

        return new ReadOnlyIterator<ClipSyncObject>() {
            @Override
            protected ClipSyncObject getNextObject() {
                if (fileIterator.hasNext()) {
                    String clipId = fileIterator.next().trim();
                    return new ClipSyncObject(clipId, CasUtil.generateRelativePath(clipId));
                }
                return null;
            }
        };
    }

    protected Iterator<ClipSyncObject> queryIterator() throws FPLibraryException {
        // verify we have appropriate privileges
        if (pool.getCapability(FPLibraryConstants.FP_CLIPENUMERATION, FPLibraryConstants.FP_ALLOWED).equals("False"))
            throw new ConfigurationException("QUERY is not supported for this pool connection.");

        final FPQueryExpression query = new FPQueryExpression();
        query.setStartTime(0);
        query.setEndTime(-1);
        query.setType(FPLibraryConstants.FP_QUERY_TYPE_EXISTING);
        query.selectField("creation.date");

        return new ReadOnlyIterator<ClipSyncObject>() {
            final FPPoolQuery poolQuery = new FPPoolQuery(pool, query);

            @Override
            protected ClipSyncObject getNextObject() {
                try {
                    FPQueryResult queryResult;
                    while (true) {
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
                                    return new ClipSyncObject(queryResult.getClipID(),
                                            CasUtil.generateRelativePath(queryResult.getClipID()));

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
                                    l4j.warn("end of query reached.");
                                    try {
                                        poolQuery.Close();
                                    } catch (Throwable t) {
                                        l4j.warn("could not close query: " + t.getMessage());
                                    }
                                    return null;

                                case FPLibraryConstants.FP_QUERY_RESULT_CODE_ABORT:
                                    // query aborted due to server side issue or start time
                                    // is later than server time.
                                    throw new RuntimeException("received FP_QUERY_RESULT_CODE_ABORT error, exiting.");

                                default:
                                    throw new RuntimeException("received error: " + queryResult.getResultCode());
                            }
                        } finally {
                            try {
                                queryResult.Close();
                            } catch (Throwable t) {
                                l4j.warn("could not close query result: " + t.getMessage());
                            }
                        }
                    } //while
                } catch (Exception e) {
                    if (lastResultCreateTime != null)
                        l4j.error("last query result create-date: " + lastResultCreateTime);
                    try {
                        poolQuery.Close();
                    } catch (Throwable t) {
                        l4j.warn("could not close query: " + t.getMessage());
                    }
                    if (e instanceof RuntimeException) throw (RuntimeException) e;
                    throw new RuntimeException(e);
                }
            }
        };
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

    public String getDeleteReason() {
        return deleteReason;
    }

    public void setDeleteReason(String deleteReason) {
        this.deleteReason = deleteReason;
    }
}
