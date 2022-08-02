/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.model;

import java.util.Objects;

public class FailedObject implements Comparable<FailedObject> {
    private final long listRowNum;
    private final String identifier;

    public FailedObject(String identifier) {
        this(-1, identifier);
    }

    public FailedObject(long listRowNum, String identifier) {
        this.listRowNum = listRowNum;
        this.identifier = identifier;
    }

    /**
     * Compare details:
     *  listRowNum is provided: compare listRowNum first, then identifier
     *  listRowNum is undefined (-1): compare identifier
     *  Mixed: FailedObject without listRowNum (-1) is before FailedObject with listRowNum.
     *
     */
    @Override
    public int compareTo(FailedObject failedObject) {
        if (this.listRowNum == failedObject.getListRowNum()) {
            return this.identifier.compareTo(failedObject.identifier);
        } else {
            return (this.listRowNum > failedObject.getListRowNum() ? 1 : -1);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FailedObject that = (FailedObject) o;
        return listRowNum == that.listRowNum && identifier.equals(that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(listRowNum, identifier);
    }

    @Override
    public String toString() {
        if (this.listRowNum == -1)
            return String.format("%s", this.identifier);
        else
            return String.format("[Line:%d] %s", this.listRowNum, this.identifier);
    }

    /**
     * NOTE: -1 indicates there was no list, or we do not know the row number
     * @see ObjectSummary#getListRowNum()
     */
    public long getListRowNum() {
        return listRowNum;
    }

    public String getIdentifier() {
        return identifier;
    }
}
