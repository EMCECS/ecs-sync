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

import java.io.*;

/**
 * Basic line iterator for text files. It trims white space, ignores blank lines and supports hash (<code>#</code>)
 * comments and escapes for including literal hashes (<code>hashed\#value</code>).
 */
public class FileLineIterator extends ReadOnlyIterator<String> {
    private static final Logger log = LoggerFactory.getLogger(FileLineIterator.class);

    BufferedReader br;
    int currentLine = 0;

    public FileLineIterator(String file) {
        try {
            if ("-".equals(file))
                br = new BufferedReader(new InputStreamReader(System.in));
            else
                br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }
    }

    public FileLineIterator(File file) {
        try {
            br = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found", e);
        }
    }

    @Override
    protected String getNextObject() {
        try {
            String line;
            do {
                line = br.readLine();
                if (line == null) break;
                currentLine++;
                line = line.replaceFirst("(?<!\\\\)#.*$", ""); // remove comment
                line = line.trim();
                line = line.replaceAll("\\\\#", "#"); // unescape hashes
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
