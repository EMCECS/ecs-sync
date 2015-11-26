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
package com.emc.ecs.sync.bean;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement(name = "JobControl")
@XmlType(propOrder = {"status", "syncThreadCount", "queryThreadCount"})
public class JobControl {
    private JobControlStatus status;
    private int syncThreadCount;
    private int queryThreadCount;

    public JobControl() {
    }

    public JobControl(JobControlStatus status, int syncThreadCount, int queryThreadCount) {
        this.status = status;
        this.syncThreadCount = syncThreadCount;
        this.queryThreadCount = queryThreadCount;
    }

    @XmlElement(name = "Status")
    public JobControlStatus getStatus() {
        return status;
    }

    public void setStatus(JobControlStatus status) {
        this.status = status;
    }

    @XmlElement(name = "SyncThreadCount")
    public int getSyncThreadCount() {
        return syncThreadCount;
    }

    public void setSyncThreadCount(int syncThreadCount) {
        this.syncThreadCount = syncThreadCount;
    }

    @XmlElement(name = "QueryThreadCount")
    public int getQueryThreadCount() {
        return queryThreadCount;
    }

    public void setQueryThreadCount(int queryThreadCount) {
        this.queryThreadCount = queryThreadCount;
    }
}
