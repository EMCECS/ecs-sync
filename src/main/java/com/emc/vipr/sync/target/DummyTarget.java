/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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

import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.util.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * Dummy target object that can be used to test sources or filter plugins.
 *
 * @author cwikj
 */
public class DummyTarget extends SyncTarget {
    private static final Logger l4j = Logger.getLogger(DummyTarget.class);

    public static final String SINK_DATA_OPTION = "sink-data";
    public static final String SINK_DATA_DESC = "Read all data from the input stream";
    private boolean sinkData;

    @Override
    public boolean canHandleTarget(String targetUri) {
        return "dummy".equals(targetUri);
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withDescription(SINK_DATA_DESC).withLongOpt(SINK_DATA_OPTION).create());
        return opts;
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        sinkData = line.hasOption(SINK_DATA_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        // No known plugins this is incompatible with.
    }

    @Override
    public void filter(SyncObject obj) {
        obj.setTargetIdentifier("file:///dev/null");
        if (sinkData && !obj.isDirectory()) {
            LogMF.debug(l4j, "Sinking source object {0}", obj.getSourceIdentifier());
            byte[] buffer = new byte[4096];
            InputStream in = obj.getInputStream();
            try {
                while (in != null && in.read(buffer) != -1) {
                    // Do nothing!
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read input stream: " + e.getMessage(), e);
            } finally {
                safeClose(in);
            }
        }
    }

    @Override
    public String getName() {
        return "Dummy Target";
    }

    @Override
    public String getDocumentation() {
        return "The dummy target simply discards any data received.  With" +
                " the --sink-data option it will also read all data from any " +
                "input streams and discard that too.  This plugin is mainly " +
                "used for testing sources and filters.  It is activated by " +
                "using the target 'dummy'";
    }

    public boolean isSinkData() {
        return sinkData;
    }

    public void setSinkData(boolean sinkData) {
        this.sinkData = sinkData;
    }
}
