/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.config;

public class ConfigurationException extends RuntimeException {
    private String errorCode;

    public ConfigurationException() {
    }

    public ConfigurationException(String errorMessage) {
        super(errorMessage);
    }

    public ConfigurationException(String errorCode, String errorMessage) {
        super(errorMessage);
        this.errorCode = errorCode;
    }

    public ConfigurationException(String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
    }

    public ConfigurationException(Throwable throwable) {
        super(throwable);
    }

    public ConfigurationException(String errorCode, String errorMessage, Throwable throwable) {
        super(errorMessage, throwable);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    @Override
    public String toString() {
        return summarize(this);
    }

    protected String summarize(Throwable t) {
        String str = errorCode != null ? "ErrorCode:" + errorCode + " ": "";
        str += t == this ? super.toString() : t.toString();
        if (t.getCause() != null && !t.getCause().equals(t)) str += "[" + summarize(t.getCause()) + "]";
        return str;
    }
}
