package com.emc.ecs.sync.storage.cas;

public interface CloseListener {
    /**
     * NOTE: implementations must not throw exceptions
     */
    void closed(String identifier);
}
