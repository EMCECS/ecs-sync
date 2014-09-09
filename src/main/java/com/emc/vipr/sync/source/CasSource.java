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
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.target.CasTarget;
import com.emc.vipr.sync.target.CuaFilesystemTarget;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.*;
import com.filepool.fplibrary.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO: make this compatible with any target
 */
public class CasSource extends SyncSource<CasSource.ClipSyncObject> {
    private static final Logger l4j = Logger.getLogger(CasSource.class);

    public static final String SOURCE_CLIP_LIST_OPTION = "source-clip-list";
    public static final String SOURCE_CLIP_LIST_DESC = "The file containing the list of clip IDs to copy (newline separated). Use - to read the list from standard input.";
    public static final String SOURCE_CLIP_LIST_ARG_NAME = "filename";

    protected static final int DEFAULT_BUFFER_SIZE = 1048576; // 1MB

    protected static final String APPLICATION_NAME = CasSource.class.getName();
    protected static final String APPLICATION_VERSION = ViPRSync.class.getPackage().getImplementationVersion();

    protected String connectionString;
    protected String clipIdFile;
    protected FPPool pool;
    protected String lastResultCreateTime;

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
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (!(target instanceof CuaFilesystemTarget) && !(target instanceof CasTarget))
            throw new ConfigurationException("CasSource is currently only compatible with CasTarget or CuaFilesystemTarget");

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
        List<ClipTag> tags = new ArrayList<>();
        try {
            // the entire clip (and all blobs) will be sent at once, so we can keep references to clips and tags open.
            // open the clip
            clip = TimingUtil.time(CasSource.this, CasUtil.OPERATION_OPEN_CLIP, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    return new FPClip(pool, syncObject.getClipId(), FPLibraryConstants.FP_OPEN_FLAT);
                }
            });

            // pull the CDF
            final FPClip fClip = clip;
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            TimingUtil.time(CasSource.this, CasUtil.OPERATION_READ_CDF, new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    fClip.RawRead(baos);
                    return null;
                }
            });
            syncObject.setClipName(clip.getName());
            syncObject.setCdfData(baos.toByteArray());
            syncObject.setSize(clip.getTotalSize());

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
                l4j.warn("could not close tag " + syncObject.getClipId() + "." + tagCount + ": " + t.getMessage());
            }
            // close blob tags
            for (ClipTag blobSync : tags) {
                try {
                    blobSync.getTag().Close();
                } catch (Throwable t) {
                    l4j.warn("could not close tag " + syncObject.getClipId() + "." + blobSync.getTagNum() + ": " + t.getMessage());
                }
            }
            // close clip
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                l4j.warn("could not close clip " + syncObject.getClipId() + ": " + t.getMessage());
            }
        }
    }

    @Override
    public Iterator<ClipSyncObject> childIterator(ClipSyncObject syncObject) {
        return null;
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

    public class ClipSyncObject extends SyncObject<ClipSyncObject> {
        private String clipId;
        private String clipName;
        private byte[] cdfData;
        private long size;
        private List<ClipTag> tags;

        public ClipSyncObject(String clipId, String relativePath) {
            super(clipId, relativePath);
            this.clipId = clipId;
        }

        @Override
        public Object getRawSourceIdentifier() {
            return sourceIdentifier;
        }

        @Override
        public boolean hasData() {
            return true;
        }

        @Override
        public long getSize() {
            return size;
        }

        @Override
        public InputStream createSourceInputStream() {
            return new ByteArrayInputStream(cdfData);
        }

        @Override
        public boolean hasChildren() {
            return false;
        }

        @Override
        public long getBytesRead() {
            long total = super.getBytesRead();
            for (ClipTag tag : tags) {
                total += tag.getBytesRead();
            }
            return total;
        }

        public String getClipId() {
            return clipId;
        }

        public String getClipName() {
            return clipName;
        }

        public void setClipName(String clipName) {
            this.clipName = clipName;
        }

        public void setCdfData(byte[] cdfData) {
            this.cdfData = cdfData;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public void setTags(List<ClipTag> tags) {
            this.tags = tags;
        }

        public List<ClipTag> getTags() {
            return tags;
        }
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
}
