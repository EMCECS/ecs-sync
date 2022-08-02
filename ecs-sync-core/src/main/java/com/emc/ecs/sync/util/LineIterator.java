/*
 * Copyright (c) 2014-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Basic line iterator for text files or streams. It trims white space, ignores blank lines and supports hash
 * (<code>#</code>) comments and escapes for including literal hashes (<code>hashed\#value</code>).
 */
public class LineIterator extends ReadOnlyIterator<String> {
    private static final Logger log = LoggerFactory.getLogger(LineIterator.class);

    boolean rawValues;
    BufferedReader br;
    int currentLine = 0;

    public LineIterator(String file) {
        this(file, false);
    }

    public LineIterator(String file, boolean rawValues) {
        try {
            if ("-".equals(file))
                br = new BufferedReader(new InputStreamReader(System.in));
            else
                br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }
        this.rawValues = rawValues;
    }

    public LineIterator(File file) {
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }
    }

    /**
     * Note: this class will *not* close the stream; you must handle that in calling code
     */
    public LineIterator(InputStream stream) {
        br = new BufferedReader(new InputStreamReader(stream));
    }

    @Override
    protected String getNextObject() {
        try {
            String line;
            do {
                line = br.readLine();
                if (line == null) break;
                currentLine++;
                if (!rawValues) { // don't do any parsing if we need raw values
                    // regex explanation:
                    // - match the whole line
                    // - end of line must contain a non-quoted, non-escaped hash (this is a comment)
                    //     - this part (the comment) is removed
                    // - matching group (part that will not be removed) contains any number of the following:
                    //     - a quoted value (may contain two-quotes to denote a quote inside the value)
                    //     - a string without quotes or hashes
                    //     - an unquoted, escaped hash (\#)
                    line = line.replaceFirst("^((?:\"[^\"]*\"|\\\\#|[^\"#])*)(?<!\\\\)#.*$", "$1"); // remove comment
                    line = line.trim();
                    // unescape hashes
                    int lastLength;
                    do {
                        lastLength = line.length();
                        line = line.replaceFirst("^((?:\"[^\"]*\"|[^\"])*?)\\\\#", "$1#");
                    } while (lastLength != line.length());
                }
            } while (line.length() == 0);

            if (line == null) {
                log.info("End of file reached");
                br.close();
            }
            return line;
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        }
    }

    public int getCurrentLine() {
        return currentLine;
    }
}
