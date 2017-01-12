/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.storage.cas;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.storage.CasConfig;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.AbstractStorage;
import com.emc.ecs.sync.storage.ObjectNotFoundException;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.*;
import com.emc.object.util.ProgressInputStream;
import com.emc.object.util.ProgressListener;
import com.filepool.fplibrary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;

public class CasStorage extends AbstractStorage<CasConfig> {
    private static final Logger log = LoggerFactory.getLogger(CasStorage.class);

    private static final String OPERATION_FETCH_QUERY_RESULT = "CasFetchQueryResult";
    private static final String OPERATION_OPEN_CLIP = "CasOpenClip";
    private static final String OPERATION_READ_CDF = "CasReadCdf";
    private static final String OPERATION_WRITE_CDF = "CasWriteCdf";
    private static final String OPERATION_STREAM_BLOB = "CasStreamBlob";
    private static final String OPERATION_WRITE_CLIP = "CasWriteClip";
    private static final String OPERATION_SIZE_CLIP = "CasGetClipSize";

    private static final int CLIP_OPTIONS = 0;

    static void safeClose(FPTag tag, String clipId, int tagNum) {
        try {
            if (tag != null) tag.Close();
        } catch (FPLibraryException e) {
            log.warn("could not close tag {}.{}: {}", clipId, tagNum, summarizeError(e));
        } catch (Throwable t) {
            log.warn("could not close tag " + clipId + "." + tagNum, t);
        }
    }

    static void safeClose(FPClip clip, String clipId) {
        try {
            if (clip != null) clip.Close();
        } catch (FPLibraryException e) {
            log.warn("could not close clip {}: {}", clipId, summarizeError(e));
        } catch (Throwable t) {
            log.warn("could not close clip " + clipId, t);
        }
    }

    static void safeClose(ClipTag tag, String clipId) {
        try {
            if (tag != null) tag.close();
        } catch (Throwable t) {
            log.warn("could not close tag " + clipId + "." + tag.getTagNum(), t);
        }
    }

    private FPPool pool;
    private String lastResultCreateTime;
    private EnhancedThreadPoolExecutor blobReadExecutor;

    @Override
    public void configure(SyncStorage source, Iterator<SyncFilter> filters, SyncStorage target) {
        super.configure(source, filters, target);

        if (this == target && !(source instanceof CasStorage))
            throw new ConfigurationException("CasStorage as a target is currently only compatible with CasStorage as a source");

        Assert.hasText(config.getConnectionString());

        try {
            if (pool == null) {
                FPPool.RegisterApplication(config.getApplicationName(), config.getApplicationVersion());
                pool = new FPPool(config.getConnectionString());
            }

            // Check connection
            FPPool.PoolInfo info = pool.getPoolInfo();
            log.info("Connected to {} ({}) using CAS v.{}",
                    info.getClusterName(), info.getClusterID(), info.getVersion());

            // verify we have appropriate privileges
            if (this == source) {
                if (pool.getCapability(FPLibraryConstants.FP_READ, FPLibraryConstants.FP_ALLOWED).equals("False"))
                    throw new ConfigurationException("READ is not supported for this pool connection");

                if (getOptions().isDeleteSource()
                        && pool.getCapability(FPLibraryConstants.FP_DELETE, FPLibraryConstants.FP_ALLOWED).equals("False"))
                    throw new ConfigurationException("DELETE is not supported for this pool connection");
            }
            if (this == target) {
                if (pool.getCapability(FPLibraryConstants.FP_WRITE, FPLibraryConstants.FP_ALLOWED).equals("False"))
                    throw new ConfigurationException("WRITE is not supported for this pool connection");
            }

        } catch (FPLibraryException e) {
            throw new ConfigurationException("error creating pool: " + summarizeError(e), e);
        }

        blobReadExecutor = new EnhancedThreadPoolExecutor(options.getThreadCount(),
                new LinkedBlockingDeque<Runnable>(options.getThreadCount()), "blob-read-pool");
    }

    @Override
    public String getRelativePath(String identifier, boolean directory) {
        return identifier;
    }

    @Override
    public String getIdentifier(String relativePath, boolean directory) {
        return relativePath;
    }

    @Override
    protected ObjectSummary createSummary(final String identifier) {
        log.debug("sizing {}...", identifier);
        ObjectSummary summary = time(new Function<ObjectSummary>() {
            @Override
            public ObjectSummary call() {
                try {
                    FPClip clip = new FPClip(pool, identifier, FPLibraryConstants.FP_OPEN_FLAT);
                    long size = clip.getTotalSize();
                    clip.Close();
                    return new ObjectSummary(identifier, false, size);
                } catch (FPLibraryException e) {
                    throw new RuntimeException(e);
                }
            }
        }, OPERATION_SIZE_CLIP);
        log.debug("size of {} is {} bytes", identifier, summary.getSize());
        return summary;
    }

    @Override
    public Iterable<ObjectSummary> allObjects() {
        return new Iterable<ObjectSummary>() {
            @Override
            public Iterator<ObjectSummary> iterator() {
                try {
                    // verify we have appropriate privileges
                    if (pool.getCapability(FPLibraryConstants.FP_CLIPENUMERATION, FPLibraryConstants.FP_ALLOWED).equals("False"))
                        throw new ConfigurationException("QUERY is not supported for this pool connection.");

                    final FPQueryExpression query = new FPQueryExpression();
                    query.setStartTime(0);
                    query.setEndTime(-1);
                    query.setType(FPLibraryConstants.FP_QUERY_TYPE_EXISTING);
                    query.selectField("creation.date");
                    query.selectField("totalsize");

                    return new ReadOnlyIterator<ObjectSummary>() {
                        final FPPoolQuery poolQuery = new FPPoolQuery(pool, query);

                        @Override
                        protected ObjectSummary getNextObject() {
                            try {
                                FPQueryResult queryResult;
                                while (true) {
                                    queryResult = time(new Callable<FPQueryResult>() {
                                        @Override
                                        public FPQueryResult call() throws Exception {
                                            return poolQuery.FetchResult();
                                        }
                                    }, OPERATION_FETCH_QUERY_RESULT);
                                    try {
                                        switch (queryResult.getResultCode()) {
                                            case FPLibraryConstants.FP_QUERY_RESULT_CODE_OK:
                                                log.debug("query result OK; creating ReadClipTask.");
                                                long totalSize = Long.parseLong(queryResult.getField("totalsize"));
                                                lastResultCreateTime = queryResult.getField("creation.date");
                                                return new ObjectSummary(queryResult.getClipID(), false, totalSize);

                                            case FPLibraryConstants.FP_QUERY_RESULT_CODE_INCOMPLETE:
                                                log.info("received FP_QUERY_RESULT_CODE_INCOMPLETE error, invalid C-Clip, trying again.");
                                                break;

                                            case FPLibraryConstants.FP_QUERY_RESULT_CODE_COMPLETE:
                                                log.info("received FP_QUERY_RESULT_CODE_COMPLETE, there should have been a previous "
                                                        + "FP_QUERY_RESULT_CODE_INCOMPLETE error reported.");
                                                break;

                                            case FPLibraryConstants.FP_QUERY_RESULT_CODE_PROGRESS:
                                                log.info("received FP_QUERY_RESULT_CODE_PROGRESS, continuing.");
                                                break;

                                            case FPLibraryConstants.FP_QUERY_RESULT_CODE_ERROR:
                                                log.info("received FP_QUERY_RESULT_CODE_ERROR error, retrying again");
                                                break;

                                            case FPLibraryConstants.FP_QUERY_RESULT_CODE_END:
                                                log.warn("end of query reached.");
                                                try {
                                                    poolQuery.Close();
                                                } catch (Throwable t) {
                                                    log.warn("could not close query: " + t.getMessage());
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
                                            log.warn("could not close query result: " + t.getMessage());
                                        }
                                    }
                                } //while
                            } catch (Exception e) {
                                if (lastResultCreateTime != null)
                                    log.error("last query result create-date: " + lastResultCreateTime);
                                try {
                                    poolQuery.Close();
                                } catch (Throwable t) {
                                    log.warn("could not close query: " + t.getMessage());
                                }
                                if (e instanceof RuntimeException) throw (RuntimeException) e;
                                throw new RuntimeException(e);
                            }
                        }
                    };
                } catch (FPLibraryException e) {
                    throw new RuntimeException(summarizeError(e), e);
                }
            }
        };
    }

    @Override
    public Iterable<ObjectSummary> children(ObjectSummary parent) {
        return new Iterable<ObjectSummary>() {
            @Override
            public Iterator<ObjectSummary> iterator() {
                return Collections.emptyIterator();
            }
        };
    }

    @Override
    public SyncObject loadObject(final String identifier) throws ObjectNotFoundException {
        int tagCount = 0;
        FPClip clip = null;
        FPTag tag = null;
        List<ClipTag> tags = null;
        try {
            // check existence first (if this is the target, it probably doesn't exist!)
            if (!FPClip.Exists(pool, identifier)) throw new ObjectNotFoundException(identifier);

            // open the clip
            final FPClip fClip = clip = TimingUtil.time(getOptions(), OPERATION_OPEN_CLIP, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    return new FPClip(pool, identifier, FPLibraryConstants.FP_OPEN_FLAT);
                }
            });

            // pull the CDF
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            TimingUtil.time(getOptions(), OPERATION_READ_CDF, new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    fClip.RawRead(baos);
                    return null;
                }
            });

            byte[] cdfData = baos.toByteArray();

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(clip.getTotalSize());
            metadata.setModificationTime(new Date(clip.getCreationDate()));

            // pull all clip tags
            tags = new ArrayList<>();
            ProgressListener listener = null;
            if (getOptions().isMonitorPerformance())
                listener = new PerformanceListener(getReadWindow());
            while ((tag = clip.FetchNext()) != null) {
                tags.add(new ClipTag(tag, tagCount++, getOptions().getBufferSize(), listener, blobReadExecutor));
            }

            return new ClipSyncObject(this, identifier, clip, cdfData, tags, metadata);
        } catch (ObjectNotFoundException e) {
            throw e;
        } catch (Exception e) {
            safeClose(tag, identifier, tagCount);
            if (tags != null) {
                for (ClipTag clipTag : tags) {
                    safeClose(clipTag, identifier);
                }
            }
            safeClose(clip, identifier);
            if (e instanceof FPLibraryException)
                throw new RuntimeException(summarizeError((FPLibraryException) e), e);
            else if (e instanceof RuntimeException) throw (RuntimeException) e;
            else throw new RuntimeException(e);
        }
    }

    @Override
    public String createObject(SyncObject object) {
        if (!(object instanceof ClipSyncObject))
            throw new UnsupportedOperationException("sync object was not a CAS clip");
        ClipSyncObject clipObject = (ClipSyncObject) object;

        final String clipId = object.getRelativePath();
        FPClip clip = null;
        FPTag tag = null;
        int targetTagNum = 0;
        try (final InputStream cdfIn = clipObject.getDataStream()) {
            // first clone the clip via CDF raw write
            clip = TimingUtil.time(getOptions(), OPERATION_WRITE_CDF, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    return new FPClip(pool, clipId, cdfIn, CLIP_OPTIONS);
                }
            });

            // next write the blobs
            for (final ClipTag sourceTag : clipObject.getTags()) {
                tag = clip.FetchNext(); // this should sync the tag indexes
                if (sourceTag.isBlobAttached()) { // only stream if the tag has a blob
                    timedStreamBlob(tag, sourceTag);
                }
                tag.Close();
                tag = null;
            }

            // finalize the clip
            final FPClip fClip = clip;
            String destClipId = TimingUtil.time(getOptions(), OPERATION_WRITE_CLIP, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return fClip.Write();
                }
            });
            if (!destClipId.equals(clipId))
                throw new RuntimeException(String.format("clip IDs do not match\n    [%s != %s]", clipId, destClipId));

            log.debug("Wrote source {} to dest {}", clipId, destClipId);

            return destClipId;
        } catch (Throwable t) {
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            if (t instanceof FPLibraryException)
                throw new RuntimeException("Failed to store object: " + summarizeError((FPLibraryException) t), t);
            throw new RuntimeException("Failed to store object: " + t.getMessage(), t);
        } finally {
            // close current tag ref
            safeClose(tag, clipId, targetTagNum);
            // close clip
            safeClose(clip, clipId);
        }
    }

    @Override
    public void updateObject(String identifier, SyncObject object) {
        throw new UnsupportedOperationException("attempt to update existing CAS clip " + identifier);
    }

    @Override
    public void delete(String identifier) {
        try {
            FPClip.AuditedDelete(pool, identifier, config.getDeleteReason(),
                    FPLibraryConstants.FP_OPTION_DEFAULT_OPTIONS);
        } catch (FPLibraryException e) {
            throw new RuntimeException(summarizeError(e), e);
        }
    }

    @Override
    public void close() {
        super.close();
        if (pool != null) try {
            pool.Close();
        } catch (Throwable t) {
            log.warn("could not close pool: " + t.getMessage());
        }
        pool = null;
        blobReadExecutor.shutdown();
    }

    private void timedStreamBlob(final FPTag tag, final ClipTag blob) throws Exception {
        TimingUtil.time(getOptions(), OPERATION_STREAM_BLOB, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                InputStream sourceStream = blob.getBlobInputStream();
                if (getOptions().isMonitorPerformance())
                    sourceStream = new ProgressInputStream(sourceStream, new PerformanceListener(getWriteWindow()));
                try (InputStream stream = sourceStream) {
                    tag.BlobWrite(stream);
                }
                return null;
            }
        });
    }

    public static String summarizeError(FPLibraryException e) {
        return String.format("CAS Error %s/%s: %s", e.getErrorCode(), e.getErrorString(), e.getMessage());
    }
}
