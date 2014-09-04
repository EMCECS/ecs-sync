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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.Iterator;

/**
 * Strips the ACL from the object.  Useful if the UIDs don't match between
 * source and target.
 *
 * @author cwikj
 */
public class StripAclFilter extends SyncFilter {
    public static final String ACTIVATION_NAME = "strip-acl";

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        return new Options();
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
    }

    @Override
    public void validateChain(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
    }

    @Override
    public void filter(SyncObject obj) {
        for (String user : obj.getMetadata().getUserAclKeys()) {
            obj.getMetadata().removeUserAclProp(user);
        }
        for (String group : obj.getMetadata().getGroupAclKeys()) {
            obj.getMetadata().removeGroupAclProp(group);
        }
        getNext().filter(obj);
    }

    @Override
    public String getName() {
        return "Strip ACL";
    }

    @Override
    public String getDocumentation() {
        return "Strips the ACL from the object between source and target.  Useful for when the UIDs don't match between Atmos systems.";
    }
}
