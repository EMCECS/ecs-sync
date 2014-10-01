package com.emc.vipr.sync.util;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * Basic line iterator for text files. It trims white space, ignores blank lines and supports hash (<code>#</code>)
 * comments and escapes for including literal hashes (<code>hashed\#value</code>).
 */
public class FileLineIterator extends ReadOnlyIterator<String> {
    private static final Logger l4j = Logger.getLogger(FileLineIterator.class);

    BufferedReader br;
    int currentLine = 0;

    public FileLineIterator(String file) {
        try {
            if ("-".equals(file))
                br = new BufferedReader(new InputStreamReader(System.in));
            else
                br = new BufferedReader(new FileReader(new File(file)));
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
                l4j.info("End of file reached");
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
