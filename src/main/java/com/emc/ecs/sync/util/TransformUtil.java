/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.vipr.transform.TransformConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public final class TransformUtil {
    private static final Logger log = LoggerFactory.getLogger(TransformUtil.class);

    public static final List<String> ENCRYPTION_METADATA_KEYS = Arrays.asList(
            TransformConstants.META_ENCRYPTION_IV,
            TransformConstants.META_ENCRYPTION_KEY_ID,
            TransformConstants.META_ENCRYPTION_OBJECT_KEY,
            TransformConstants.META_ENCRYPTION_UNENC_SHA1,
            TransformConstants.META_ENCRYPTION_UNENC_SIZE,
            TransformConstants.META_ENCRYPTION_META_SIG
    );

    public static String getEncryptionSpec(SyncObject obj) {
        String transformSpec = obj.getMetadata().getUserMetadataValue(TransformConstants.META_TRANSFORM_MODE);
        log.debug(obj + " - transformSpec: " + transformSpec);

        if (transformSpec != null) {
            for (String transform : transformSpec.split("\\|")) {
                if (transform.startsWith(TransformConstants.ENCRYPTION_CLASS)) {
                    return transform;
                }
            }
        }
        return null;
    }

    public static String getLastTransform(SyncObject obj) {
        String transformSpec = obj.getMetadata().getUserMetadataValue(TransformConstants.META_TRANSFORM_MODE);
        log.debug(obj + " - transformSpec: " + transformSpec);

        if (transformSpec != null && transformSpec.contains("|"))
            transformSpec = transformSpec.substring(transformSpec.lastIndexOf("|") + 1);

        return transformSpec;
    }

    private TransformUtil() {
    }
}
