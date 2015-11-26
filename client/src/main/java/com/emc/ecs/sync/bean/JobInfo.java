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

public class JobInfo {
    private Integer jobId;
    private JobControlStatus status;

    public JobInfo() {
    }

    public JobInfo(Integer jobId, JobControlStatus status) {
        this.jobId = jobId;
        this.status = status;
    }

    @XmlElement(name = "JobId")
    public Integer getJobId() {
        return jobId;
    }

    public void setJobId(Integer jobId) {
        this.jobId = jobId;
    }

    @XmlElement(name = "Status")
    public JobControlStatus getStatus() {
        return status;
    }

    public void setStatus(JobControlStatus status) {
        this.status = status;
    }
}
