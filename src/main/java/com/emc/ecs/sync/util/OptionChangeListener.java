package com.emc.ecs.sync.util;

import com.emc.ecs.sync.config.SyncOptions;

public interface OptionChangeListener {
    void optionsChanged(SyncOptions options);
}
