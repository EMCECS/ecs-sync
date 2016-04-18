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
package com.emc.ecs.sync.rest;

import com.emc.ecs.sync.service.SyncRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class ErrorStreamWriter implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ErrorStreamWriter.class);

    public static final int BUFFER_SIZE = 128 * 1024;
    public static final String DATE_FORMAT = "";

    private Iterable<SyncRecord> records;
    private PipedInputStream readStream;
    private Writer writer;
    private DateFormat formatter = new SimpleDateFormat(DATE_FORMAT);

    public ErrorStreamWriter(Iterable<SyncRecord> records) throws IOException {
        this.records = records;
        this.readStream = new PipedInputStream(BUFFER_SIZE);
        this.writer = new OutputStreamWriter(new PipedOutputStream(readStream));
    }

    @Override
    public void run() {
        try {
            // header
            writeCsvRow("Source Path", "Target Path", "Directory", "Size", "Transfer Start", "Retry Count", "Error Message");

            // rows
            for (SyncRecord record : records) {
                String tStart = record.getTransferStart() == null ? null : formatter.format(record.getTransferStart());
                writeCsvRow(record.getSourceId(), record.getTargetId(), record.isDirectory(), record.getSize(),
                        tStart, record.getRetryCount(), record.getErrorMessage());
            }

            writer.flush();
            writer.close();
        } catch (IOException e) {
            // don't close the stream, so the read end will get an exception and know there was a problem
            log.warn("Exception in stream writer!", e);
        }
    }

    protected void writeCsvRow(Object... values) throws IOException {
        for (int i = 0; i < values.length; i++) {
            if (i > 0) writer.write(",");
            if (values[i] != null) writer.write("\"" + escape(values[i].toString()) + "\"");
        }
        writer.write("\n");
    }

    protected String escape(String value) {
        return value.replace("\"", "\"\"");
    }

    public InputStream getReadStream() {
        return readStream;
    }
}
