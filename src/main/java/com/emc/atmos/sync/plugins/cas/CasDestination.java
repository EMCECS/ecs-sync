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
import com.emc.atmos.sync.plugins.DestinationPlugin;
import com.emc.atmos.sync.plugins.SyncObject;
import com.emc.atmos.sync.plugins.SyncPlugin;
import com.emc.atmos.sync.util.TimingUtil;
import com.filepool.fplibrary.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TODO: make this compatible with any source
 * TODO: more logging
 */
public class CasDestination extends DestinationPlugin implements InitializingBean {
    private static final Logger l4j = Logger.getLogger(CasDestination.class);

    protected static final String APPLICATION_NAME = AtmosSync2.class.getSimpleName();
    protected static final String APPLICATION_VERSION = AtmosSync2.class.getPackage().getImplementationVersion();
    protected static final int CLIP_OPTIONS = 0;

    protected String connectionString;
    protected FPPool pool;

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
                    return new FPClip(pool, clipSync.getClipId(), clipSync.getInputStream(), CLIP_OPTIONS);
                }
            });
            clipSync.setDestURI(CasUtil.generateSyncUri(connectionString, clipSync.getClipId()));

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
            if (!destClipId.equals(clipSync.getClipId()))
                throw new RuntimeException(String.format("clip IDs do not match\n    [%s != %s]",
                        clipSync.getClipId(), destClipId));

            clipSync.setDestURI(CasUtil.generateSyncUri(connectionString, clipSync.getClipId()));

            LogMF.debug(l4j, "Wrote source {0} to dest {1}", clipSync.getSourceURI(), clipSync.getDestURI());

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
                l4j.warn("could not close tag " + clipSync.getClipId() + "." + targetTagNum, t);
            }
            // close clip
            try {
                if (clip != null) clip.Close();
            } catch (Throwable t) {
                l4j.warn("could not close clip " + clipSync.getClipId(), t);
            }
        }
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        return new Options();
    }

    @Override
    public boolean parseOptions(CommandLine line) {
        if (line.hasOption(CommonOptions.DESTINATION_OPTION)) {
            Pattern p = Pattern.compile(CasUtil.URI_PATTERN);
            String dest = line.getOptionValue(CommonOptions.DESTINATION_OPTION);
            Matcher m = p.matcher(dest);
            if (!m.matches()) {
                LogMF.debug(l4j, "{0} does not match {1}", dest, p);
                return false;
            }

            connectionString = dest.replaceFirst("^cas://", "");

            // create and verify CAS connection
            try {
                afterPropertiesSet();
            } catch (FPLibraryException e) {
                throw new RuntimeException(e);
            }

            return true;
        }

        return false;
    }

    @Override
    public void afterPropertiesSet() throws FPLibraryException {
        Assert.hasText(connectionString);

        FPPool.RegisterApplication(APPLICATION_NAME, APPLICATION_VERSION);

        // Check connection
        pool = new FPPool(connectionString);
        FPPool.PoolInfo info = pool.getPoolInfo();
        LogMF.info(l4j, "Connected to destination: {0} ({1}) using CAS v.{2}",
                info.getClusterName(), info.getClusterID(), info.getVersion());

        // verify we have appropriate privileges
        if (pool.getCapability(FPLibraryConstants.FP_WRITE, FPLibraryConstants.FP_ALLOWED).equals("False"))
            throw new IllegalArgumentException("WRITE is not supported for this pool connection");
    }

    @Override
    public void validateChain(SyncPlugin first) {
        if (!(first instanceof CasSource))
            throw new UnsupportedOperationException("CasDestination is currently only compatible with CasSource");
    }

    @Override
    public String getName() {
        return "CAS Destination";
    }

    @Override
    public String getDocumentation() {
        return "The CAS destination plugin is triggered by the destination pattern:\n" +
                "cas://host[:port][,host[:port]...]?name=<name>,secret=<secret>\n" +
                "or cas://host[:port][,host[:port]...]?<pea_file>\n" +
                "Note that <name> should be of the format <subtenant_id>:<uid>. " +
                "This is passed to the CAS API as the connection string " +
                "(you can use primary=, secondary=, etc. in the server hints).\n" +
                "When used with CasSource, clips are transferred using their " +
                "raw CDFs to facilitate transparent data migration.";
    }

    public String getConnectionString() {
        return connectionString;
    }

    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    protected void timedStreamBlob(final FPTag tag, final ClipTag blob) throws Exception {
        TimingUtil.time(CasDestination.this, CasUtil.OPERATION_STREAM_BLOB, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                blob.writeToTag(tag);
                return null;
            }
        });
    }
}
