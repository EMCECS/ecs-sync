/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.util;

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;

public final class SyncUtil {
    private static final Logger l4j = Logger.getLogger(SyncUtil.class);

    public static void consumeStream(InputStream input) {
        try {
            byte[] devNull = new byte[16 * 1024];
            int c = 0;
            while (c != -1) {
                c = input.read(devNull);
            }
        } catch (IOException e) {
            throw new RuntimeException("error consuming stream", e);
        } finally {
            safeClose(input);
        }
    }

    public static void safeClose(Closeable closeable) {
        try {
            if (closeable != null) closeable.close();
        } catch (Throwable t) {
            l4j.warn("could not close resource", t);
        }
    }

    public static String summarize(Throwable t) {
        Throwable cause = getCause(t);
        StringBuilder summary = new StringBuilder();
        summary.append(MessageFormat.format("[{0}] {1}", t, cause));
        StackTraceElement[] elements = cause.getStackTrace();
        for (int i = 0; i < 15 && i < elements.length; i++) {
            summary.append("\n    at ").append(elements[i]);
        }
        return summary.toString();
    }

    public static Throwable getCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) cause = cause.getCause();
        return cause;
    }

    private SyncUtil() {
    }
}
