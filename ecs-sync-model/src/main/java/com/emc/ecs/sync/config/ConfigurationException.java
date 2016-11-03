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
package com.emc.ecs.sync.config;

public class ConfigurationException extends RuntimeException {
    public ConfigurationException() {
    }

    public ConfigurationException(String s) {
        super(s);
    }

    public ConfigurationException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ConfigurationException(Throwable throwable) {
        super(throwable);
    }

    @Override
    public String toString() {
        return summarize(this);
    }

    protected String summarize(Throwable t) {
        String str = t == this ? super.toString() : t.toString();
        if (t.getCause() != null && !t.getCause().equals(t)) str += "[" + summarize(t.getCause()) + "]";
        return str;
    }
}
