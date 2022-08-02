/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.AbstractCliTest;
import com.emc.ecs.sync.config.ConfigUtil;
import com.emc.ecs.sync.config.SyncConfig;
import com.emc.ecs.sync.config.storage.CasConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CasCliTest extends AbstractCliTest {
    @Test
    public void testCasCli() {
        String conString1 = "10.6.143.90,10.6.143.91?/file.pea";
        String conString2 = "10.6.143.97:3218,10.6.143.98:3218?name=e332b0c325c444438f51bfd8d25c6b55:test,secret=XvfPa442hanJ7GzQasyx+j5X9kY=";
        String appName = "";
        String appVersion = "";
        String deleteReason = "";

        String[] args = new String[]{
                "-source", "cas://" + conString1,
                "-target", "cas://" + conString2,
                "--source-application-name", appName,
                "--source-application-version", appVersion,
                "--source-delete-reason", deleteReason,
        };

        SyncConfig syncConfig = parseSyncConfig(args);

        Object source = syncConfig.getSource();
        Assertions.assertNotNull(source, "source is null");
        Assertions.assertTrue(source instanceof CasConfig, "source is not FilesystemSource");
        CasConfig castSource = (CasConfig) source;

        Object target = syncConfig.getTarget();
        Assertions.assertNotNull(target, "target is null");
        Assertions.assertTrue(target instanceof CasConfig, "target is not FilesystemTarget");
        CasConfig castTarget = (CasConfig) target;

        Assertions.assertEquals("cas:hpp://" + conString1, ConfigUtil.generateUri(castSource, false));
        Assertions.assertEquals("cas:hpp://" + conString2, ConfigUtil.generateUri(castTarget, false));
        Assertions.assertEquals("hpp://" + conString1, castSource.getConnectionString(), "source conString mismatch");
        Assertions.assertEquals(appName, castSource.getApplicationName(), "source appName mismatch");
        Assertions.assertEquals(appVersion, castSource.getApplicationVersion(), "source appVersion mismatch");
        Assertions.assertEquals(deleteReason, castSource.getDeleteReason(), "source deleteReason mismatch");
        Assertions.assertEquals("hpp://" + conString2, castTarget.getConnectionString(), "target conString mismatch");
    }
}
