package com.emc.vipr.sync.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by joshia7 on 6/26/15.
 */
public class SwiftUtil {

    public static final String URI_PREFIX = "swift:";
    public static final String URI_PATTERN = "^" + URI_PREFIX + "(?:(http|https)://)?([^:]+):([^@]+)@?(?:([^/:]*?)(:[0-9]+)?)?(?:/(.*))?$";
    public static final String PATTERN_DESC = URI_PREFIX + "[http[s]://]username:password@[host[:port]][/root-prefix]";

    public static SwiftUri parseUri(String uri) {
        Pattern p = Pattern.compile(URI_PATTERN);
        Matcher m = p.matcher(uri);
        if (!m.matches()) {
            throw new ConfigurationException(String.format("URI does not match %s pattern (%s)", URI_PREFIX, PATTERN_DESC));
        }

        SwiftUri swiftUri = new SwiftUri();

        swiftUri.protocol = m.group(1);
        String host = m.group(4);
        int port = -1;
        if (m.group(5) != null) {
            port = Integer.parseInt(m.group(5).substring(1));
        }

        if (host != null && !host.isEmpty()) {
            try {
                swiftUri.endpoint = new URI(swiftUri.protocol, null, host, port, null, null, null).toString();
            } catch (URISyntaxException e) {
                throw new ConfigurationException("invalid endpoint URI", e);
            }
        }

        swiftUri.username = m.group(2);
        swiftUri.password = m.group(3);

        if (m.group(6) != null)
            swiftUri.rootKey = m.group(6);

        return swiftUri;
    }


    public static class SwiftUri {
        public String protocol;
        public String endpoint;
        public String username;
        public String password;
        public String rootKey;

        public String toUri() {
            String uri = URI_PREFIX + username + ":" + password + "@";
            if (endpoint != null) uri += endpoint;
            if (rootKey != null) uri += rootKey;
            return uri;
        }
    }
}


