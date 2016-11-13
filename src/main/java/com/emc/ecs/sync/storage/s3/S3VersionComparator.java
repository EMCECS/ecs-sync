package com.emc.ecs.sync.storage.s3;

import java.util.Comparator;

public class S3VersionComparator implements Comparator<S3ObjectVersion> {
    @Override
    public int compare(S3ObjectVersion o1, S3ObjectVersion o2) {
        int result = o1.getMetadata().getModificationTime().compareTo(o2.getMetadata().getModificationTime());
        if (result == 0) result = o1.getVersionId().compareTo(o2.getVersionId());
        return result;
    }
}
