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
package com.emc.ecs.sync.util;

import java.util.Iterator;
import java.util.List;

public class CombinedIterator<T> extends ReadOnlyIterator<T> {
    private List<? extends Iterator<T>> iterators;
    private int currentIterator = 0;

    public CombinedIterator(List<? extends Iterator<T>> iterators) {
        this.iterators = iterators;
    }

    @Override
    protected T getNextObject() {
        while (currentIterator < iterators.size()) {
            if (iterators.get(currentIterator).hasNext()) return iterators.get(currentIterator).next();
            currentIterator++;
        }

        return null;
    }
}
