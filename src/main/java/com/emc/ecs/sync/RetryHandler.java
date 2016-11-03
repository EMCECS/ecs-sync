package com.emc.ecs.sync;

import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.storage.SyncStorage;

public interface RetryHandler {
    void submitForRetry(SyncStorage source, ObjectContext objectContext, Throwable t) throws Throwable;
}
