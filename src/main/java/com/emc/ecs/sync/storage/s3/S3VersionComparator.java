/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
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
