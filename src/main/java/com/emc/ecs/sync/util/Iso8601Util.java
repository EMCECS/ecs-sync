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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.WeakHashMap;

public final class Iso8601Util {
    private static final String ISO_8601_DATE_Z = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String ISO_8601_DATE_MICRO_Z = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'";
    private static final String[] PARSE_FORMATS = new String[] { ISO_8601_DATE_Z, ISO_8601_DATE_MICRO_Z};


    private static final Logger log = LoggerFactory.getLogger(Iso8601Util.class);

    private static final ThreadLocal<WeakHashMap<String,DateFormat>> formatCache = new ThreadLocal<>();

    public static Date parse(String string) {
        if (string == null) return null;

        for(String fmt : PARSE_FORMATS) {
            DateFormat df = getFormat(fmt);
            try {
                Date d = df.parse(string);
                return d;
            } catch (Exception e) {
                log.debug("Could not parse date {} with format {}: {}", string, fmt, e.getMessage());
            }
        }

        log.warn("Could not parse date {} with any formats.", string);
        return null;
    }

    public static String format(Date date) {
        if (date == null) return null;
        return getFormat(ISO_8601_DATE_Z).format(date);
    }

    private static DateFormat getFormat(String formatString) {
        if(formatCache.get() == null) {
            formatCache.set(new WeakHashMap<String, DateFormat>());
        }

        DateFormat format = formatCache.get().get(formatString);
        if (format == null) {
            format = new SimpleDateFormat(formatString);
            format.setTimeZone( TimeZone.getTimeZone( "UTC" ));
            formatCache.get().put(formatString, format);
        }
        return format;
    }

    private Iso8601Util() {}
}
