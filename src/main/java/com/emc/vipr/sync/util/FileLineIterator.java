package com.emc.vipr.sync.util;

import org.apache.log4j.Logger;

import java.io.*;

public class FileLineIterator extends ReadOnlyIterator<String> {
    private static final Logger l4j = Logger.getLogger(FileLineIterator.class);

    BufferedReader br;

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
            String line = br.readLine();
            if (line == null) {
                l4j.info("End of file reached");
                br.close();
            }
            return line;
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        }
    }
}
