package com.emc.ecs.sync;

import com.emc.ecs.sync.config.SyncConfig;

public class AbstractCliTest {
    protected SyncConfig parseSyncConfig(String[] cliArgs) {
        return CliHelper.parseSyncConfig(CliHelper.parseCliConfig(cliArgs), cliArgs);
    }
}
