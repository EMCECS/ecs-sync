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
package com.emc.ecs.sync.test;

import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Option;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;

import javax.xml.bind.annotation.XmlRootElement;

public class DelayFilter extends AbstractFilter<DelayFilter.DelayConfig> {
    @Override
    public void filter(ObjectContext objectContext) {
        try {
            Thread.sleep(config.getDelayMs());
            getNext().filter(objectContext);
        } catch (InterruptedException e) {
            throw new RuntimeException("interrupted during wait", e);
        }
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    @XmlRootElement
    @FilterConfig(cliName = "delay")
    public static class DelayConfig {
        private int delayMs;

        @Option
        public int getDelayMs() {
            return delayMs;
        }

        public void setDelayMs(int delayMs) {
            this.delayMs = delayMs;
        }

        public DelayConfig withDelayMs(int delayMs) {
            setDelayMs(delayMs);
            return this;
        }
    }
}
