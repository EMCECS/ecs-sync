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
package com.emc.vipr.sync.filter;

import com.emc.vipr.sync.model.AtmosMetadata;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author cwikj
 */
public class MetadataFilter extends SyncFilter {
    private static final Logger l4j = Logger.getLogger(MetadataFilter.class);

    public static final String ACTIVATION_NAME = "metadata";

    public static final String ADD_META_OPTION = "add-metadata";
    public static final String ADD_META_DESC = "Adds a regular metadata element to items";
    public static final String ADD_META_ARG = "name=value,name=value,...";

    public static final String ADD_LISTABLE_META_OPTION = "add-listable-meta";
    public static final String ADD_LISTABLE_META_DESC = "Adds a listable metadata element to items";

    Map<String, String> metadata;
    Map<String, String> listableMetadata;

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(ADD_META_OPTION).withDescription(ADD_META_DESC)
                .hasArgs().withArgName(ADD_META_ARG).withValueSeparator(',').create());
        opts.addOption(new OptionBuilder().withLongOpt(ADD_LISTABLE_META_OPTION).withDescription(ADD_LISTABLE_META_DESC)
                .hasArgs().withArgName(ADD_META_ARG).withValueSeparator(',').create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        metadata = new HashMap<>();
        listableMetadata = new HashMap<>();

        if (line.hasOption(ADD_META_OPTION)) {
            String[] values = line.getOptionValues(ADD_META_OPTION);

            for (String value : values) {
                String[] parts = value.split("=", 2);
                if (parts.length != 2) {
                    // Empty value?
                    metadata.put(parts[0], "");
                } else {
                    metadata.put(parts[0], parts[1]);
                }
            }
        }

        if (line.hasOption(ADD_LISTABLE_META_OPTION)) {
            String[] values = line.getOptionValues(ADD_LISTABLE_META_OPTION);

            for (String value : values) {
                String[] parts = value.split("=", 2);
                if (parts.length != 2) {
                    // Empty value?
                    listableMetadata.put(parts[0], "");
                } else {
                    listableMetadata.put(parts[0], parts[1]);
                }
            }
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
    }

    @Override
    public void filter(SyncObject obj) {
        SyncMetadata meta = obj.getMetadata();
        for (String key : metadata.keySet()) {
            l4j.debug(String.format("adding metadata %s=%s to %s", key, metadata.get(key), obj.getSourceIdentifier()));
            meta.setUserMetadataProp(key, metadata.get(key));
        }

        if (meta instanceof AtmosMetadata) {
            for (String key : listableMetadata.keySet()) {
                l4j.debug(String.format("adding listable metadata %s=%s to %s", key, metadata.get(key), obj));
                ((AtmosMetadata) meta).setUserMetadataProp(key, metadata.get(key), true);
            }
        } else {
            l4j.info(String.format("could not add listable meta to non-atmos source object %s", obj));
        }

        getNext().filter(obj);
    }

    @Override
    public String getName() {
        return "Metadata Filter";
    }

    @Override
    public String getDocumentation() {
        return "Allows adding regular and listable (Atmos only) metadata to each object";
    }
}
