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

import com.emc.vipr.sync.model.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.Iterator;

/**
 * @author cwikj
 */
public class OverrideMimetypeFilter extends SyncFilter {
    public static final String ACTIVATION_NAME = "override-mimetype";

    private static final String OVERRIDE_MIMETYPE_OPTION = "override-mimetype";
    private static final String OVERRIDE_MIMETYPE_DESC = "specifies the mimetype to use when an object" +
            "has no default mimetype.";
    private static final String OVERRIDE_MIMETYPE_ARG_NAME = "mimetype";

    private static final String FORCE_MIMETYPE_OPTION = "force-mimetype";
    private static final String FORCE_MIMETYPE_DESC = "If specified, the " +
            "mimetype will be overwritten regardless of its prior value.";

    private String mimeType;
    private boolean force;

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(OVERRIDE_MIMETYPE_OPTION).withDescription(OVERRIDE_MIMETYPE_DESC)
                .hasArg().withArgName(OVERRIDE_MIMETYPE_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(FORCE_MIMETYPE_OPTION).withDescription(FORCE_MIMETYPE_DESC).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        if (!line.hasOption(OVERRIDE_MIMETYPE_OPTION))
            throw new ConfigurationException("you must provide an " + OVERRIDE_MIMETYPE_OPTION);
        mimeType = line.getOptionValue(OVERRIDE_MIMETYPE_OPTION);

        force = line.hasOption(FORCE_MIMETYPE_OPTION);
    }

    @Override
    public void validateChain(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (mimeType == null)
            throw new ConfigurationException("you must provide a mimetype");
    }

    @Override
    public void filter(SyncObject obj) {
        if (force) {
            obj.getMetadata().setContentType(mimeType);
        } else {
            if (obj.getMetadata().getContentType() == null ||
                    obj.getMetadata().getContentType().equals("application/octet-stream")) {
                obj.getMetadata().setContentType(mimeType);
            }
        }

        getNext().filter(obj);
    }

    @Override
    public String getName() {
        return "Override Mimetype";
    }

    @Override
    public String getDocumentation() {
        return "This plugin allows you to override the default mimetype " +
                "of objects getting transferred.  It is useful for instances " +
                "where the mimetype of an object cannot be inferred from " +
                "its extension or is nonstandard (not in Java's " +
                "mime.types file).  You can also use the force option to " +
                "override the mimetype of all objects.";
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
