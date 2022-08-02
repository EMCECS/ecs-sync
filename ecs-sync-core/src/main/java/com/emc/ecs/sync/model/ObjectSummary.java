/*
 * Copyright (c) 2016-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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

public class ObjectSummary {
    private final String identifier;
    private final boolean directory;
    private final long size;
    private String listFileRow;
    private long listRowNum = -1; // -1 means there was no list, or we do not know the line number

    public ObjectSummary(String identifier, boolean directory, long size) {
        this.identifier = identifier;
        this.directory = directory;
        this.size = size;
    }

    public String getIdentifier() {
        return identifier;
    }

    public boolean isDirectory() {
        return directory;
    }

    public long getSize() {
        return size;
    }

    /**
     * When a source-list-file is provided listing the objects to sync, the full line of data associated with this
     * object is available here
     */
    public String getListFileRow() {
        return listFileRow;
    }

    public void setListFileRow(String listFileRow) {
        this.listFileRow = listFileRow;
    }

    /**
     * This is the row number that this object appears in the source list (or list file).
     * For example, if this object appeared on the 10th row of the list file, this property will be <code>10</code>.
     * If there was no list, or we do not know the row number, this will be <code>-1</code>.
     */
    public long getListRowNum() {
        return listRowNum;
    }

    public void setListRowNum(long listRowNum) {
        this.listRowNum = listRowNum;
    }
}
