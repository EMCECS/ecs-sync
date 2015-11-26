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

import java.util.Arrays;
import java.util.List;

public enum ObjectStatus {
    Error("Error"),
    RetryQueue("Retry Queue"),
    InTransfer("In Transfer"),
    InVerification("In Verification"),
    Transferred("Transferred"),
    Verified("Verified");

    public static final List<ObjectStatus> COMPLETE_STATUSES = Arrays.asList(Transferred, Verified);

    public static ObjectStatus fromValue(String value) {
        for (ObjectStatus e : values()) {
            if (e.getValue().equals(value)) return e;
        }
        return null;
    }

    public static boolean isFinal(ObjectStatus status) {
        return COMPLETE_STATUSES.contains(status);
    }

    private String value;

    ObjectStatus(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
