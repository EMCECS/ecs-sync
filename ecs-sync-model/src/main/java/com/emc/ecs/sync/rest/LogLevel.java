/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.rest;

import javax.xml.bind.annotation.XmlEnum;

@XmlEnum
public enum LogLevel {
    silent(false), quiet(false), verbose(true), debug(true);

    private boolean includeStackTrace = false;

    LogLevel(boolean includeStackTrace) {
        this.includeStackTrace = includeStackTrace;
    }

    public boolean isIncludeStackTrace() {
        return includeStackTrace;
    }
}
