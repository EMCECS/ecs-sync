package com.emc.ecs.sync.storage.s3;

/**
 * Label interface for S3 compatible storage. Used primarily for determining if delete-markers will be handled properly
 * when versions are included
 */
public interface S3Storage {
    String PROP_OBJECT_VERSIONS = "s3.objectVersions";
}
