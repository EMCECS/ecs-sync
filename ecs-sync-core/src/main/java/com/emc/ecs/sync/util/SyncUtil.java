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
package com.emc.ecs.sync.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.List;

public final class SyncUtil {
    private static final Logger log = LoggerFactory.getLogger(SyncUtil.class);

    public static void consumeAndCloseStream(InputStream stream) {
        try (InputStream input = stream) {
            byte[] devNull = new byte[32 * 1024];
            int c = 0;
            while (c != -1) {
                c = input.read(devNull);
            }
        } catch (IOException e) {
            throw new RuntimeException("error consuming stream", e);
        }
    }

    public static byte[] readAsBytes(InputStream in) throws IOException {
        try {
            byte[] buffer = new byte[4096];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            boolean var3 = false;

            int c;
            while((c = in.read(buffer)) != -1) {
                baos.write(buffer, 0, c);
            }

            baos.close();
            byte[] var4 = baos.toByteArray();
            return var4;
        } finally {
            if (in != null) {
                in.close();
            }

        }
    }

    public static long copy(InputStream is, OutputStream os, long maxBytes) throws IOException {
        byte[] buffer = new byte[65536];
        long count = 0L;

        try {
            while(count < maxBytes) {
                int maxRead = (int)Math.min((long)buffer.length, maxBytes - count);
                int read;
                if (-1 == (read = is.read(buffer, 0, maxRead))) {
                    break;
                }

                os.write(buffer, 0, read);
                count += (long)read;
            }
        } finally {
            try {
                is.close();
            } catch (Throwable var18) {
                log.warn("could not close stream", var18);
            }

            try {
                os.close();
            } catch (Throwable var17) {
                log.warn("could not close stream", var17);
            }

        }

        return count;
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

    public static String join(List<?> objects, String delimiter) {
        String result = "";
        for (Object object : objects) {
            if (result.length() > 0) result += delimiter;
            result += object.toString();
        }
        return result;
    }

    /**
     * like {@link java.io.File#File(String, String)}, but normalized to forward slashes to avoid Windows issues
     */
    public static String combinedPath(String parentPath, String childPath) {
        if (childPath == null || childPath.trim().length() == 0) return parentPath;

        if (parentPath == null || parentPath.trim().length() == 0) return childPath;

        // strip path separator from parent
        char lastParentChar = parentPath.charAt(parentPath.length() - 1);
        // NOTE: we are not using File.pathSeparatorChar in case paths were generated on a different system
        if (lastParentChar == '/' || lastParentChar == '\\')
            parentPath = parentPath.substring(0, parentPath.length() - 1);

        // strip path separator from child
        char firstChildChar = childPath.charAt(0);
        if (firstChildChar == '/' || firstChildChar == '\\')
            childPath = childPath.substring(1);

        // normalize to forward slash
        return parentPath + "/" + childPath;
    }

    public static String parentPath(String path) {
        if (path == null || path.trim().length() == 0) return null;

        // strip trailing separator
        char lastPathChar = path.charAt(path.length() - 1);
        // NOTE: we are not using File.pathSeparatorChar in case paths were generated on a different system
        if (lastPathChar == '/' || lastPathChar == '\\')
            path = path.substring(0, path.length() - 1);

        int lastSeparatorIndex = path.lastIndexOf('/');
        if (lastSeparatorIndex == -1) lastSeparatorIndex = path.lastIndexOf('\\');
        if (lastSeparatorIndex == -1) return null;

        if (lastSeparatorIndex == 0) return path.substring(0, 1); // the parent is the root dir
        else return path.substring(0, lastSeparatorIndex);
    }

    private SyncUtil() {
    }
}
