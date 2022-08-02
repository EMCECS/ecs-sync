/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.test;

import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StartNotifyFilter extends AbstractFilter<StartNotifyFilter.StartNotifyConfig> {
    @Override
    public void filter(ObjectContext objectContext) {
        getConfig().signalStart();
        getNext().filter(objectContext);
    }

    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    @XmlRootElement
    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    @FilterConfig(cliName = "start-notify")
    public static class StartNotifyConfig {
        private final CountDownLatch latch = new CountDownLatch(1);

        public void waitForStart() throws InterruptedException {
            latch.await();
        }

        public boolean waitForStart(long timeout, TimeUnit timeUnit) throws InterruptedException {
            return latch.await(timeout, timeUnit);
        }

        void signalStart() {
            latch.countDown();
        }
    }
}
