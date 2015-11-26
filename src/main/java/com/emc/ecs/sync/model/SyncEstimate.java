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
package com.emc.ecs.sync.model;

import java.util.concurrent.atomic.AtomicLong;

public class SyncEstimate {
    private AtomicLong totalObjectCount = new AtomicLong(0);
    private AtomicLong totalByteCount = new AtomicLong(0);

    public void incTotalObjectCount(long num) {
        totalObjectCount.addAndGet(num);
    }

    public void incTotalByteCount(long num) {
        totalByteCount.addAndGet(num);
    }

    public long getTotalObjectCount() {
        return totalObjectCount.get();
    }

    public long getTotalByteCount() {
        return totalByteCount.get();
    }
}
