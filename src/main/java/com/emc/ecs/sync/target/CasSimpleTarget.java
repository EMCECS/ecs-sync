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
package com.emc.ecs.sync.target;

import com.emc.ecs.sync.EcsSync;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.ClipSyncObject;
import com.emc.ecs.sync.model.object.FileSyncObject;
import com.emc.ecs.sync.model.object.SimpleClipSyncObject;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.CasSource;
import com.emc.ecs.sync.source.FilesystemSource;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.util.CasUtil;
import com.emc.ecs.sync.util.ClipTag;
import com.emc.ecs.sync.util.ConfigurationException;
import com.emc.ecs.sync.util.TimingUtil;
import com.emc.object.util.ProgressInputStream;
import com.emc.object.util.ProgressListener;
import com.filepool.fplibrary.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CasSimpleTarget extends SyncTarget {

    private static final Logger log = LoggerFactory.getLogger(CasSimpleTarget.class);

    public static final String DEFAULT_TOP_TAG = "x-emc-data";
    public static final String TAG_OPTION = "target-top-tag";
    public static final String TAG_DESC = "Specifies the target top tag name to use";
    public static final String TAG_ARG_NAME = "tag";
    public static final String TAG_PATTERN = "^(_|[a-zA-Z])+([a-zA-Z0-9]|\\.|-)*$";

    public static final String CLIP_NAME_OPTION = "clip-name";
    public static final String CLIP_NAME_DESC = "Specifies a generic name to be set on each clip.";
    public static final String CLIP_NAME_ARG_NAME = "name";

    public static final String RETENTION_CLASS_OPTION = "retention-class";
    public static final String RETENTION_CLASS_DESC = "Specifies the target retention class to specify on the clip.";
    public static final String RETENTION_CLASS_ARG_NAME = "class";

    public static final String RETENTION_PERIOD_OPTION = "retention-period";
    public static final String RETENTION_PERIOD_DESC = "Specifies the retention period, in seconds, to set on target clips.";
    public static final String RETENTION_PERIOD_ARG_NAME = "seconds";

    public static final String EBR_RETENTION_PERIOD_OPTION = "ebr-retention-period";
    public static final String EBR_RETENTION_PERIOD_DESC = "Specifies the event based retention period, in seconds, to set on target clips.";
    public static final String EBR_RETENTION_PERIOD_ARG_NAME = "seconds";

    public static final String EMBEDDED_DATA_THRESHOLD_OPTION = "embedded-data-threshold";
    public static final String EMBEDDED_DATA_THRESHOLD_DESC = "A value of 102400 bytes or less indicating the threshold of embedding data into the clip's top tag.";
    public static final String EMBEDDED_DATA_THRESHOLD_ARG_NAME = "size-in-bytes";

    public static final String URI_PREFIX = "cas-simple:";
    public static final String URI_PATTERN = "^" + URI_PREFIX + "hpp://([^/]*?)(:[0-9]+)?(,([^/]*?)(:[0-9]+)?)*\\?.*$";
    public static final String OPERATION_TOTAL = "TotalTime";

    public static final String OPERATION_STREAM_BLOB = "CasStreamBlob";
    public static final String OPERATION_CREATE_CLIP = "CasCreateClip";
    public static final String OPERATION_WRITE_CLIP = "CasWriteClip";

    public static final String APPLICATION_NAME = CasSimpleTarget.class.getName();
    public static final String APPLICATION_VERSION = EcsSync.class.getPackage().getImplementationVersion();

    private String connectionString;
    private FPPool pool;
    private FPRetentionClass rClass;

    private String topTagName = DEFAULT_TOP_TAG;
    private String retentionClass;
    private String clipName;
    private int blobEmbedThreshold = 0;
    private int retentionPeriod;
    private int retentionPeriodEbr;

    @Override
    public boolean canHandleTarget(String targetUri) { return targetUri.startsWith(URI_PREFIX); }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(Option.builder().longOpt(TAG_OPTION).desc(TAG_DESC)
                .hasArg().argName(TAG_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(RETENTION_CLASS_OPTION).desc(RETENTION_CLASS_DESC)
                .hasArg().argName(RETENTION_CLASS_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(RETENTION_PERIOD_OPTION).desc(RETENTION_PERIOD_DESC)
                .hasArg().argName(RETENTION_PERIOD_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(EBR_RETENTION_PERIOD_OPTION).desc(EBR_RETENTION_PERIOD_DESC)
                .hasArg().argName(EBR_RETENTION_PERIOD_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(EMBEDDED_DATA_THRESHOLD_OPTION).desc(EMBEDDED_DATA_THRESHOLD_DESC)
                .hasArg().argName(EMBEDDED_DATA_THRESHOLD_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(CLIP_NAME_OPTION).desc(CLIP_NAME_DESC)
                .hasArg().argName(CLIP_NAME_ARG_NAME).build());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        Pattern p;
        Matcher m;

        p = Pattern.compile(this.URI_PATTERN);
        m = p.matcher(targetUri);
        if (!m.matches())
            throw new ConfigurationException(String.format("%s does not match %s", targetUri, p));

        connectionString = targetUri.replaceFirst("^" + this.URI_PREFIX, "");

        if (line.hasOption(CLIP_NAME_OPTION))
            clipName = line.getOptionValue(CLIP_NAME_OPTION);
        if (line.hasOption(RETENTION_CLASS_OPTION))
            retentionClass = line.getOptionValue(RETENTION_CLASS_OPTION);
        if (line.hasOption(RETENTION_PERIOD_OPTION))
            retentionPeriod = Integer.parseInt(line.getOptionValue(RETENTION_PERIOD_OPTION));
        if (line.hasOption(EBR_RETENTION_PERIOD_OPTION))
            retentionPeriodEbr = Integer.parseInt(line.getOptionValue(EBR_RETENTION_PERIOD_OPTION));
        if (line.hasOption(EMBEDDED_DATA_THRESHOLD_OPTION)) {
            blobEmbedThreshold = Integer.parseInt(line.getOptionValue(EMBEDDED_DATA_THRESHOLD_OPTION));
            if (blobEmbedThreshold < 0 || blobEmbedThreshold > 102400) {
                throw new ConfigurationException(String.format("%s is not within threshold boundaries 0-102400 bytes.", Integer.toString(blobEmbedThreshold)));
            }
        }
        if (line.hasOption(TAG_OPTION)) {
            topTagName = line.getOptionValue(TAG_OPTION);
            p = Pattern.compile(this.TAG_PATTERN);
            m = p.matcher(topTagName);
            if(!m.matches()) {
                throw new ConfigurationException(String.format("%s does not match %s", topTagName, p));
            }
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.hasText(connectionString);
        Assert.isTrue(topTagName.matches(this.TAG_PATTERN), topTagName + " is not a valid tag name.");

        if (source instanceof CasSource)
            throw new ConfigurationException("CasSimpleTarget is currently not compatible with CasSource");

        try {
            FPPool.RegisterApplication(APPLICATION_NAME, APPLICATION_VERSION);
            if (blobEmbedThreshold > 0) {
                // need to set before the pool is opened.
                FPPool.setGlobalOption(FPLibraryConstants.FP_OPTION_EMBEDDED_DATA_THRESHOLD, blobEmbedThreshold);
                pool = new FPPool(connectionString);
                pool.setOption(FPLibraryConstants.FP_OPTION_PREFETCH_SIZE, blobEmbedThreshold + 2);
            } else {
                pool = new FPPool(connectionString);
            }

            // Check connection
            FPPool.PoolInfo info = pool.getPoolInfo();
            log.info("Connected to target: {} ({}) using CAS v.{}",
                    info.getClusterName(), info.getClusterID(), info.getVersion());

            // verify we have appropriate privileges
            if (pool.getCapability(FPLibraryConstants.FP_WRITE, FPLibraryConstants.FP_ALLOWED).equals("False"))
                throw new IllegalArgumentException("WRITE is not supported for this pool connection");

            FPRetentionClassContext context = null;
            try {
                if (retentionClass != null) {
                    context = pool.getRetentionClassContext();
                    int numClasses = context.getNumClasses();
                    if (numClasses > 0) {
                        FPRetentionClass tempRetClass = context.getFirst();
                        while (tempRetClass != null) {
                            String name = tempRetClass.getName();
                            if (name.equals(retentionClass)) {
                                rClass = tempRetClass;
                                break;
                            }
                            tempRetClass.close();
                            tempRetClass = context.getNext();
                        }
                    }
                    context.close();
                    if (rClass != null) {
                        log.info("Located retention class: {} with {} seconds specified as retention period.",
                                rClass.getName(), rClass.getPeriod());
                    } else {
                        throw new ConfigurationException("Unable to locate the specified retention class: " + retentionClass);
                    }
                }
            } catch (FPLibraryException e) {
                throw new RuntimeException("Error attempting to retrieve retention class: " + CasUtil.summarizeError(e));
            } finally {
                if(context != null) context.close();
            }
        } catch (Throwable t) {
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            if (t instanceof FPLibraryException)
                throw new RuntimeException("error creating pool: " + CasUtil.summarizeError((FPLibraryException) t), t);
            throw new RuntimeException("error creating pool: " + t.getMessage(), t);
        }
    }

    @Override
    public void filter(SyncObject obj){
        timeOperationStart(this.OPERATION_TOTAL);

        if ((obj instanceof ClipSyncObject))
            throw new UnsupportedOperationException("sync object is not supported.");

        if (obj.isDirectory()) {
            log.debug("Target is root folder; skipping");
            return;
        }

        FPClip clip = null;
        FPTag tag_top = null;
        FPTag tag_data = null;
        FPTag tag_user = null;
        String destClipId;

        try (final InputStream data = obj.getInputStream()) {
            // create the new clip
            clip = TimingUtil.time(this, this.OPERATION_CREATE_CLIP, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    if(clipName != null) {
                        return new FPClip(pool, clipName);
                    } else {
                        return new FPClip(pool);
                    }
                }
            });

            // retention
            if (rClass != null) clip.setRetentionClass(rClass);
            if (retentionPeriod > 0) clip.setRetentionPeriod(retentionPeriod);
            if (retentionPeriodEbr > 0) clip.enableEBRWithPeriod(retentionPeriodEbr);

            // open top-level root tag
            tag_top = clip.getTopTag();

            // create top tag
            tag_data = new FPTag(tag_top, topTagName);

            // collect system metadata and write to top tag.
            SyncMetadata smd = obj.getMetadata();
            tag_data.setAttribute("Content-Type", smd.getContentType());
            tag_data.setAttribute("Content-Length", smd.getContentLength());
            tag_data.setAttribute("x-emc-sync-mtime", smd.getModificationTime().getTime());
            tag_data.setAttribute("x-emc-sync-path", obj.getRelativePath());

            // DST specific attribute
            tag_data.setAttribute("AuditInfo", obj.getRelativePath());

            final FPTag fTag = tag_data;
            TimingUtil.time(this, this.OPERATION_STREAM_BLOB, (Callable) new Callable<Void>(){
                @Override
                public Void call() throws Exception {
                    ProgressListener targetProgress = isMonitorPerformance() ? new CasSimpleTargetProgress() : null;
                    InputStream stream = data;
                    if (targetProgress != null) stream = new ProgressInputStream(stream, targetProgress);
                    fTag.BlobWrite(stream);
                    return null;
                }
            });

            // write user meta data into individual tags
            Map<String, SyncMetadata.UserMetadata> umd = smd.getUserMetadata();
            for (SyncMetadata.UserMetadata meta : umd.values()) {
                try {
                    tag_user = new FPTag(tag_top, "x-emc-user-meta");
                    tag_user.setAttribute("name", meta.getKey());
                    tag_user.setAttribute("value", meta.getValue());
                } catch(Exception e) {
                    throw e;
                } finally {
                    if (tag_user != null) tag_user.Close();
                }
            }

            final FPClip fClip = clip;
            destClipId = TimingUtil.time(this, this.OPERATION_WRITE_CLIP, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return fClip.Write();
                }
            });

            obj.setTargetIdentifier(destClipId);

            log.debug("Wrote source {} to dest {}", obj.getSourceIdentifier(), obj.getTargetIdentifier());

        } catch (Throwable t) {
            timeOperationFailed(this.OPERATION_TOTAL);
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            if (t instanceof FPLibraryException)
                throw new RuntimeException("Failed to store object: " + CasUtil.summarizeError((FPLibraryException) t), t);
            throw new RuntimeException("Failed to store object: " + t.getMessage(), t);
        } finally {
            // close current tag ref
            try {
                if (tag_data != null) tag_data.Close();
            } catch (Throwable t) {
                log.warn("Could not close tag_data: " + obj.getRawSourceIdentifier() + ".", t);
            }
            // close top tag ref
            try {
                if (tag_top != null) tag_top.Close();
            } catch (Throwable t) {
                log.warn("Could not close tag_top: " + obj.getRawSourceIdentifier() + ".", t);
            }
            // close clip
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                log.warn("Could not close clip: " + obj.getRawSourceIdentifier(), t);
            }
        }
    }

    private class CasSimpleTargetProgress implements ProgressListener {

        @Override
        public void progress(long completed, long total) {

        }

        @Override
        public void transferred(long size) {
            if(getWritePerformanceCounter() != null) {
                getWritePerformanceCounter().increment(size);
            }
        }
    }

    @Override
    public SyncObject reverseFilter(final SyncObject obj) {
        if ((obj instanceof ClipSyncObject)) throw new UnsupportedOperationException("sync object is not supported.");

        String clipId = obj.getTargetIdentifier();
        if (clipId.isEmpty() || clipId == null) {
            throw new UnsupportedOperationException("target identifier not present in sync object.");
        }
        return new SimpleClipSyncObject(this, pool, clipId, obj.getRelativePath());
    }

    @Override
    public String getName() {
        return "CAS Simple Target";
    }

    @Override
    public String getDocumentation() {
        return "The CAS Simple target plugin is triggered by the target pattern:\n" +
                "cas-simple:hpp://host[:port][,host[:port]...]?name=<name>,secret=<secret>\n" +
                "or cas-simple:hpp://host[:port][,host[:port]...]?<pea_file>\n" +
                "Note that <name> should be of the format <subtenant_id>:<uid> " +
                "when connecting to an Atmos system. " +
                "This is passed to the CAS API as the connection string " +
                "(you can use primary=, secondary=, etc. in the server hints).\n" +
                "When used with CasSource, clips are transferred using their " +
                "raw CDFs to facilitate transparent data migration.\n" +
                "NOTE: verification of CAS objects (using --verify or --verify-only) " +
                "will only verify the CDF and not blob data!";
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (rClass != null) try {
            rClass.close();
        } catch (Throwable t) {
            log.warn("could not close retention class: " + t.getMessage());
        }
        rClass = null;
        if (pool != null) try {
            pool.Close();
        } catch (Throwable t) {
            log.warn("could not close pool: " + t.getMessage());
        }
        pool = null;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    public void setTopTagName (String topTagName) {
        this.topTagName = topTagName;
    }

    public String getTopTagName () { return topTagName; }

    public void setRetentionClass (String retentionClass) {
        this.retentionClass = retentionClass;
    }

    public String getRetentionClass () { return retentionClass; }

    public void setRetentionPeriod (int retentionPeriod) {
        this.retentionPeriod = retentionPeriod;
    }

    public int getRetentionPeriod () { return retentionPeriod; }

    public void setRetentionPeriodEbr (int retentionPeriodEbr) {
        this.retentionPeriodEbr = retentionPeriodEbr;
    }

    public int getRetentionPeriodEbr () { return retentionPeriodEbr; }

    public void setBlobEmbedThreshold (int blobEmbedThreshold) {
        this.blobEmbedThreshold = blobEmbedThreshold;
    }

    public int getBlobEmbedThreshold () { return blobEmbedThreshold; }

    public void setClipName (String clipName) { this.clipName = clipName; }

    public String getClipName() { return clipName; }
}
