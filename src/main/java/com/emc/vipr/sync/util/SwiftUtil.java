package com.emc.vipr.sync.util;

/**
 * Created by joshia7 on 6/26/15.
 */
public class SwiftUtil {

    public static final String URI_PREFIX = "swift:";

    public static class SwiftUri {
        public String protocol;
        public String endpoint;
        public String accessKey;
        public String secretKey;
        public String rootKey;

        public String toUri() {
            String uri = URI_PREFIX + accessKey + ":" + secretKey + "@";
            if (endpoint != null) uri += endpoint;
            if (rootKey != null) uri += rootKey;
            return uri;
        }
    }
}


