/*
 * Copyright (c) 2015-2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
import com.emc.ecs.sync.config.annotation.Option;
import com.emc.ecs.sync.filter.AbstractFilter;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DelayFilter extends AbstractFilter<DelayFilter.DelayConfig> {
    @Override
    public void filter(ObjectContext objectContext) {
        try {
            Thread.sleep(config.getDelayMs());

            if (config.getDataStreamDelayMs() > 0) {
                InputStream dataStream = objectContext.getObject().getDataStream();

                // add a delay to the first read of the object's data stream
                objectContext.getObject().setDataStream(new DelayedInputStream(dataStream, config.getDataStreamDelayMs()));
            }

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
    @XmlType(namespace = "http://www.emc.com/ecs/sync/model")
    @FilterConfig(cliName = "delay")
    public static class DelayConfig {
        private int delayMs;
        private int dataStreamDelayMs;

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

        public int getDataStreamDelayMs() {
            return dataStreamDelayMs;
        }

        public void setDataStreamDelayMs(int dataStreamDelayMs) {
            this.dataStreamDelayMs = dataStreamDelayMs;
        }

        public DelayConfig withDataStreamDelayMs(int dataStreamDelayMs) {
            setDataStreamDelayMs(dataStreamDelayMs);
            return this;
        }
    }

    static class DelayedInputStream extends FilterInputStream {
        private volatile int firstReadDelayMs;

        public DelayedInputStream(InputStream in, int firstReadDelayMs) {
            super(in);
            this.firstReadDelayMs = firstReadDelayMs;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            delayIfNecessary();
            return super.read(b, off, len);
        }

        private void delayIfNecessary() {
            if (firstReadDelayMs > 0) {
                synchronized (this) {
                    if (firstReadDelayMs > 0) {
                        try {
                            Thread.sleep(firstReadDelayMs);
                        } catch (InterruptedException ignored) {
                        }
                        firstReadDelayMs = 0;
                    }
                }
            }
        }
    }
}
