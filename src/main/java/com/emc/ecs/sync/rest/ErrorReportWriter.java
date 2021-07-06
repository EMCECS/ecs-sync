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
package com.emc.ecs.sync.rest;

import com.emc.ecs.sync.service.SyncRecord;

import java.io.IOException;

public class ErrorReportWriter extends AbstractCsvWriter<SyncRecord> {
    public ErrorReportWriter(Iterable<SyncRecord> records) throws IOException {
        super(records);
    }

    @Override
    protected String[] getHeaders(SyncRecord record) {
        return new String[]{"Source ID", "Target ID", "Directory", "Size", "Transfer Start", "Retry Count", "Error Message"};
    }

    @Override
    protected Object[] getColumns(SyncRecord record) {
        String target = record.getTargetId() == null ? "" : record.getTargetId();
        String tStart = record.getTransferStart() == null ? "" : formatter.format(record.getTransferStart());
        String error = record.getErrorMessage() == null ? "" : record.getErrorMessage();
        return new Object[]{record.getSourceId(), target, record.isDirectory(), record.getSize(),
                tStart, record.getRetryCount(), error};
    }
}
