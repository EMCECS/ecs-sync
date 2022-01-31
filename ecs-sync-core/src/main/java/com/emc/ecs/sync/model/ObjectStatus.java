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

public enum ObjectStatus {
    Error("Error", false),
    RetryQueue("Retry Queue", false),
    Queue("Queue", false),
    InTransfer("In Transfer", false),
    InVerification("In Verification", false),
    Transferred("Transferred", true),
    Verified("Verified", true);

    public static ObjectStatus fromValue(String value) {
        for (ObjectStatus e : values()) {
            if (e.getValue().equals(value)) return e;
        }
        return null;
    }

    private String value;
    private boolean success;

    ObjectStatus(String value, boolean success) {
        this.value = value;
        this.success = success;
    }

    public String getValue() {
        return value;
    }

    public boolean isSuccess() {
        return success;
    }
}
