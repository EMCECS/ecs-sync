/*
 * Copyright 2013 EMC Corporation. All Rights Reserved.
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
package com.emc.atmos.sync.util;

import org.apache.log4j.LogMF;
import org.apache.log4j.Logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public final class Iso8601Util {
    private static final String ISO_8601_DATE_Z = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    private static final Logger l4j = Logger.getLogger(Iso8601Util.class);

    private static final ThreadLocal<DateFormat> iso8601Format = new ThreadLocal<DateFormat>();

    public static Date parse(String string) {
        try {
            if (string == null) return null;
            return getFormat().parse(string);
        } catch (Exception e) {
            LogMF.warn( l4j, "Could not parse date {0}: {1}", string, e.getMessage() );
            return null;
        }
    }

    public static String format(Date date) {
        if (date == null) return null;
        return getFormat().format(date);
    }

    private static DateFormat getFormat() {
        DateFormat format = iso8601Format.get();
        if (format == null) {
            format = new SimpleDateFormat(ISO_8601_DATE_Z);
            format.setTimeZone( TimeZone.getTimeZone( "UTC" ));
            iso8601Format.set(format);
        }
        return format;
    }

    private Iso8601Util() {}
}
