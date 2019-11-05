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
package com.emc.ecs.sync.model;

public class ObjectSummary {
    private String identifier;
    private boolean directory;
    private long size;
    private String listFileRow;

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
}
