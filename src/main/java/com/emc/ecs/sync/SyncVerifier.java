package com.emc.ecs.sync;

import com.emc.ecs.sync.model.SyncObject;

public interface SyncVerifier {
    void verify(SyncObject sourceObject, SyncObject targetObject);
}
