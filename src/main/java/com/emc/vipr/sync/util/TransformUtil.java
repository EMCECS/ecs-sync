package com.emc.vipr.sync.util;

import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.transform.TransformConstants;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public final class TransformUtil {
    private static final Logger l4j = Logger.getLogger(TransformUtil.class);

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
        l4j.debug(obj + " - transformSpec: " + transformSpec);

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
        l4j.debug(obj + " - transformSpec: " + transformSpec);

        if (transformSpec != null && transformSpec.contains("|"))
            transformSpec = transformSpec.substring(transformSpec.lastIndexOf("|") + 1);

        return transformSpec;
    }

    private TransformUtil() {
    }
}
