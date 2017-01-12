package com.emc.ecs.sync.storage;

import com.emc.ecs.sync.SyncPlugin;
import com.emc.ecs.sync.model.ObjectSummary;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.util.PerformanceWindow;

public interface SyncStorage<C> extends SyncPlugin<C> {
    /**
     * Given an object's absolute storage identifier, return the appropriate relative path based on the storage config
     *
     * @param directory some plugins may vary the identifier if the object is a directory (Atmos, S3, etc.)
     */
    String getRelativePath(String identifier, boolean directory);

    /**
     * Given an object's relative path, return the absolute storage identifier based on the storage config
     *
     * @param directory some plugins may vary the identifier if the object is a directory (Atmos, S3, etc.)
     */
    String getIdentifier(String relativePath, boolean directory);

    /**
     * When a source-list-file is provided listing all of the objects to sync, the source storage plugin must parse
     * each line in the file and create a fully populated ObjectSummary instance representing that object.
     * Implementations *must* return a valid ObjectSummary instance, even if the object doesn't exist or the format
     * is invalid (that will be discovered later)
     */
    ObjectSummary parseListLine(String listLine);

    /**
     * Implement to return all root objects in this storage based on the configuration. If there is no heirarchy,
     * this will return all enumerable objects in the storage.
     */
    Iterable<ObjectSummary> allObjects();

    /**
     * Implement to return the children of the specified parent object. This method should always return a valid
     * iterator (which can be empty).
     */
    Iterable<ObjectSummary> children(ObjectSummary parent);

    /**
     * Locates the object represeand loads the specified object and then initializes the context from this storage system
     */
    SyncObject loadObject(String identifier) throws ObjectNotFoundException;

    /**
     * Writes the specified object as new in this storage system. Returns the resulting system-specific identifier
     */
    String createObject(SyncObject object);

    /**
     * Updates the existing object with the specified identifier in this storage system
     */
    void updateObject(String identifier, SyncObject object);

    /**
     * Implement this method to support object deletion. This is a per-object operation. If you do not wish to support
     * deletion, throw UnsupportedOperationException
     */
    void delete(String identifier);

    PerformanceWindow getReadWindow();

    PerformanceWindow getWriteWindow();

    /**
     * return the current read transfer rate of the plugin in bytes/s.
     */
    long getReadRate();

    /**
     * return the current write transfer rate of the plugin in bytes/s.
     */
    long getWriteRate();
}
