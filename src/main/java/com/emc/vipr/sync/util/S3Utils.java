/**
 *
 */
package com.emc.vipr.sync.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class S3Utils {
    /**
     * This pattern is used to activate the S3 plugins
     */
    public static final String URI_PREFIX = "s3:";
    public static final String URI_PATTERN = "^" + URI_PREFIX + "(?:(http|https)://)?([^:]+):([a-zA-Z0-9\\+/=]+)@?(?:([^/]*?)(:[0-9]+)?)?(/.*)?$";
    public static final String PATTERN_DESC = URI_PREFIX + "[http[s]://]access_key:secret_key@[host[:port]][/root-prefix]";

    public static S3Uri parseUri(String uri) {
        Pattern p = Pattern.compile(URI_PATTERN);
        Matcher m = p.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }

        S3Uri s3Uri = new S3Uri();

        s3Uri.protocol = m.group(1);
        String host = m.group(4);
        int port = -1;
        if (m.group(5) != null) {
            port = Integer.parseInt(m.group(5).substring(1));
        }

        if (host != null && !host.isEmpty()) {
            try {
                s3Uri.endpoint = new URI(s3Uri.protocol, null, host, port, null, null, null).toString();
            } catch (URISyntaxException e) {
                throw new ConfigurationException("invalid endpoint URI", e);
            }
        }

        s3Uri.accessKey = m.group(2);
        s3Uri.secretKey = m.group(3);

        if (m.group(6) != null)
            s3Uri.rootKey = m.group(6);

        return s3Uri;
    }

    public static class S3Uri {
        public String protocol;
        public String endpoint;
        public String accessKey;
        public String secretKey;
        public String rootKey;
    }

    private S3Utils() {
    }
}
