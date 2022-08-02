/*
 * Copyright (c) 2016-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.AbstractPlugin;
import com.emc.ecs.sync.config.annotation.Option;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PluginUtilTest {
    @Test
    public void testGetConfigClass() throws Exception {
        Class<TestConfig> configClass = PluginUtil.configClassFor(TestPlugin.class);

        Assertions.assertNotNull(configClass);
    }

    static class TestPlugin extends AbstractPlugin<TestConfig> {
    }

    static class TestConfig {
        private String location;

        @Option(description = "My Location")
        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }
    }
}
