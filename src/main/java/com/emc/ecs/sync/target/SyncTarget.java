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

import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.filter.SyncFilter;

/**
 * A SyncTarget functions very similarly to a SyncFilter, but exists at the end of the chain.  Implement sync logic in
 * the {@link #filter(SyncObject)} method.
 */
public abstract class SyncTarget extends SyncFilter {
    protected String targetUri;

    /**
     * return true if this target implementation can handle the specified target parameter (passed on the command line)
     *
     * @param targetUri the target parameter passed on the command line (i.e.
     *                  "atmos:http://user:key@node1.company.com")
     * @return true if the plugin should be used to handle the specified target
     */
    public abstract boolean canHandleTarget(String targetUri);

    @Override
    public final String getActivationName() {
        return null;
    }

    @Override
    public final SyncFilter getNext() {
        throw new UnsupportedOperationException("sync targets do not have a \"next\"");
    }

    @Override
    public final void setNext(SyncFilter next) {
        throw new UnsupportedOperationException("sync targets do not have a \"next\"");
    }

    @Override
    public String summarizeConfig() {
        return super.summarizeConfig()
                + " - targetUri: " + targetUri + "\n";
    }

    public String getTargetUri() {
        return targetUri;
    }

    public void setTargetUri(String targetUri) {
        this.targetUri = targetUri;
    }
}
