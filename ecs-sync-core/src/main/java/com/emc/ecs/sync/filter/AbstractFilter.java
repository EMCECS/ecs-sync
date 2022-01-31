/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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

import com.emc.ecs.sync.AbstractPlugin;

public abstract class AbstractFilter<C> extends AbstractPlugin<C> implements SyncFilter<C> {
    private SyncFilter next;

    @Override
    public SyncFilter getNext() {
        return next;
    }

    @Override
    public void setNext(SyncFilter next) {
        this.next = next;
    }
}
