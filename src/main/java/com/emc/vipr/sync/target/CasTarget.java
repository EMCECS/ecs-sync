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
package com.emc.vipr.sync.target;

import com.emc.vipr.sync.ViPRSync;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.object.ClipSyncObject;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.CasSource;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.CasUtil;
import com.emc.vipr.sync.util.ClipTag;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.TimingUtil;
import com.filepool.fplibrary.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.util.Assert;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO: make this compatible with any source
 */
public class CasTarget extends SyncTarget {
    private static final Logger l4j = Logger.getLogger(CasTarget.class);

    protected static final String APPLICATION_NAME = CasTarget.class.getName();
    protected static final String APPLICATION_VERSION = ViPRSync.class.getPackage().getImplementationVersion();
    protected static final int CLIP_OPTIONS = 0;

    protected String connectionString;
    protected FPPool pool;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return targetUri.matches(CasUtil.URI_PATTERN);
    }

    @Override
    public Options getCustomOptions() {
        return new Options();
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        Pattern p = Pattern.compile(CasUtil.URI_PATTERN);
        Matcher m = p.matcher(targetUri);
        if (!m.matches())
            throw new ConfigurationException(String.format("%s does not match %s", targetUri, p));

        connectionString = targetUri.replaceFirst("^" + CasUtil.URI_PREFIX, "");
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (!(source instanceof CasSource))
            throw new ConfigurationException("CasTarget is currently only compatible with CasSource");

        Assert.hasText(connectionString);

        try {
            FPPool.RegisterApplication(APPLICATION_NAME, APPLICATION_VERSION);

            // Check connection
            pool = new FPPool(connectionString);
            FPPool.PoolInfo info = pool.getPoolInfo();
            LogMF.info(l4j, "Connected to target: {0} ({1}) using CAS v.{2}",
                    info.getClusterName(), info.getClusterID(), info.getVersion());

            // verify we have appropriate privileges
            if (pool.getCapability(FPLibraryConstants.FP_WRITE, FPLibraryConstants.FP_ALLOWED).equals("False"))
                throw new IllegalArgumentException("WRITE is not supported for this pool connection");
        } catch (FPLibraryException e) {
            throw new RuntimeException("error creating pool", e);
        }
    }

    @Override
    public void filter(final SyncObject obj) {
        timeOperationStart(CasUtil.OPERATION_TOTAL);

        if (!(obj instanceof ClipSyncObject))
            throw new UnsupportedOperationException("sync object was not a CAS clip");
        final ClipSyncObject clipSync = (ClipSyncObject) obj;

        FPClip clip = null;
        FPTag tag = null;
        int targetTagNum = 0;
        try {
            // first clone the clip via CDF raw write
            clip = TimingUtil.time(this, CasUtil.OPERATION_WRITE_CDF, new Callable<FPClip>() {
                @Override
                public FPClip call() throws Exception {
                    return new FPClip(pool, clipSync.getRawSourceIdentifier(), clipSync.getInputStream(), CLIP_OPTIONS);
                }
            });
            clipSync.setTargetIdentifier(clipSync.getRawSourceIdentifier());

            // next write the blobs

            for (ClipTag sourceTag : clipSync.getTags()) {
                tag = clip.FetchNext(); // this should sync the tag indexes
                if (sourceTag.isBlobAttached()) { // only stream if the tag has a blob
                    timedStreamBlob(tag, sourceTag);
                }
                tag.Close();
                tag = null;
            }

            final FPClip fClip = clip;
            String destClipId = TimingUtil.time(this, CasUtil.OPERATION_WRITE_CLIP, new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return fClip.Write();
                }
            });
            if (!destClipId.equals(clipSync.getRawSourceIdentifier()))
                throw new RuntimeException(String.format("clip IDs do not match\n    [%s != %s]",
                        clipSync.getRawSourceIdentifier(), destClipId));

            LogMF.debug(l4j, "Wrote source {0} to dest {1}", clipSync.getSourceIdentifier(), clipSync.getTargetIdentifier());

            timeOperationComplete(CasUtil.OPERATION_TOTAL);
        } catch (Throwable t) {
            timeOperationFailed(CasUtil.OPERATION_TOTAL);
            if (t instanceof RuntimeException) throw (RuntimeException) t;
            throw new RuntimeException("Failed to store object: " + t.getMessage(), t);
        } finally {
            // close current tag ref
            try {
                if (tag != null) tag.Close();
            } catch (Throwable t) {
                l4j.warn("could not close tag " + clipSync.getRawSourceIdentifier() + "." + targetTagNum, t);
            }
            // close clip
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                l4j.warn("could not close clip " + clipSync.getRawSourceIdentifier(), t);
            }
        }
    }

    /**
     * NOTE: the returned object will only contain the clip's CDF and no tags or data
     */
    @Override
    public SyncObject reverseFilter(final SyncObject obj) {
        FPClip clip = null;
        try {
            if (!(obj instanceof ClipSyncObject))
                throw new UnsupportedOperationException("sync object was not a CAS clip");
            final ClipSyncObject sourceObj = (ClipSyncObject) obj;

            ClipSyncObject clipObject = new ClipSyncObject(sourceObj.getRawSourceIdentifier(),
                    CasUtil.generateRelativePath(sourceObj.getRawSourceIdentifier()));

            clip = CasUtil.openClip(this, pool, sourceObj.getRawSourceIdentifier());
            CasUtil.hydrateClipData(this, clipObject, clip);

            return clipObject;
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException(e);
        } finally {
            // close clip
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                l4j.warn("could not close clip " + obj.getTargetIdentifier() + ": " + t.getMessage());
            }
        }
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

    @Override
    public String getName() {
        return "CAS Target";
    }

    @Override
    public String getDocumentation() {
        return "The CAS target plugin is triggered by the target pattern:\n" +
                "cas://host[:port][,host[:port]...]?name=<name>,secret=<secret>\n" +
                "or cas://host[:port][,host[:port]...]?<pea_file>\n" +
                "Note that <name> should be of the format <subtenant_id>:<uid> " +
                "when connecting to an Atmos system. " +
                "This is passed to the CAS API as the connection string " +
                "(you can use primary=, secondary=, etc. in the server hints).\n" +
                "When used with CasSource, clips are transferred using their " +
                "raw CDFs to facilitate transparent data migration.\n" +
                "NOTE: verification of CAS objects (using --verify or --verify-only) " +
                "will only verify the CDF and not blob data!";
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    protected void timedStreamBlob(final FPTag tag, final ClipTag blob) throws Exception {
        TimingUtil.time(CasTarget.this, CasUtil.OPERATION_STREAM_BLOB, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                blob.writeToTag(tag);
                return null;
            }
        });
    }
}
