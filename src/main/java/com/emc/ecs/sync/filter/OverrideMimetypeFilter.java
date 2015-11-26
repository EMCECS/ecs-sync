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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.target.SyncTarget;
import com.emc.ecs.sync.util.ConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
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
        opts.addOption(Option.builder().longOpt(OVERRIDE_MIMETYPE_OPTION).desc(OVERRIDE_MIMETYPE_DESC)
                .hasArg().argName(OVERRIDE_MIMETYPE_ARG_NAME).build());
        opts.addOption(Option.builder().longOpt(FORCE_MIMETYPE_OPTION).desc(FORCE_MIMETYPE_DESC).build());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        if (line.hasOption(OVERRIDE_MIMETYPE_OPTION))
            mimeType = line.getOptionValue(OVERRIDE_MIMETYPE_OPTION);

        force = line.hasOption(FORCE_MIMETYPE_OPTION);
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
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

    // TODO: if verification ever includes mime-type, reverse mime type
    // TODO: how to keep track of old values to revert
    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        return getNext().reverseFilter(obj);
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
