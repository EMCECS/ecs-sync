/*
 * Copyright (c) 2017-2021 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.rest;

import com.emc.ecs.sync.service.ExtendedSyncRecord;
import com.emc.ecs.sync.service.SyncRecord;

import java.io.IOException;

public class DbDumpWriter extends AbstractCsvWriter<SyncRecord> {
    public DbDumpWriter(Iterable<SyncRecord> records) throws IOException {
        super(records);
    }

    @Override
    protected String[] getHeaders(SyncRecord firstRecord) {
        if (firstRecord instanceof ExtendedSyncRecord) {
            return new String[]{"Source ID", "Target ID", "Directory", "Size", "Source mtime", "Source MD5",
                    "Source Retention End-time", "Target mtime", "Target MD5", "Target Retention End-time",
                    "Status", "Transfer Start", "Transfer Complete", "Verify Start",
                    "Verify Complete", "Retry Count", "Error Message", "First Error Message", "Source Deleted"};
        } else {
            return new String[]{"Source ID", "Target ID", "Directory", "Size", "Source mtime",
                    "Status", "Transfer Start", "Transfer Complete", "Verify Start",
                    "Verify Complete", "Retry Count", "Error Message", "Source Deleted"};
        }
    }

    @Override
    protected Object[] getColumns(SyncRecord record) {
        String target = record.getTargetId() == null ? "" : record.getTargetId();
        String mtime = record.getMtime() == null ? "" : formatter.format(record.getMtime());
        String tStart = record.getTransferStart() == null ? "" : formatter.format(record.getTransferStart());
        String tComp = record.getTransferComplete() == null ? "" : formatter.format(record.getTransferComplete());
        String vStart = record.getVerifyStart() == null ? "" : formatter.format(record.getVerifyStart());
        String vComp = record.getVerifyComplete() == null ? "" : formatter.format(record.getVerifyComplete());
        String error = record.getErrorMessage() == null ? "" : record.getErrorMessage();
        if (record instanceof ExtendedSyncRecord) {
            ExtendedSyncRecord extRecord = (ExtendedSyncRecord) record;
            String sourceMd5 = extRecord.getSourceMd5() == null ? "" : extRecord.getSourceMd5();
            String sourceRetentionEndTime = extRecord.getSourceRetentionEndTime() == null
                    ? "" : formatter.format(extRecord.getSourceRetentionEndTime());
            String targetMtime = extRecord.getTargetMtime() == null
                    ? "" : formatter.format(extRecord.getTargetMtime());
            String targetMd5 = extRecord.getTargetMd5() == null ? "" : extRecord.getTargetMd5();
            String targetRetentionEndTime = extRecord.getTargetRetentionEndTime() == null
                    ? "" : formatter.format(extRecord.getTargetRetentionEndTime());
            String firstError = extRecord.getFirstErrorMessage() == null ? "" : extRecord.getFirstErrorMessage();
            return new Object[]{record.getSourceId(), target, record.isDirectory(),
                    record.getSize(), mtime, sourceMd5, sourceRetentionEndTime, targetMtime,
                    targetMd5, targetRetentionEndTime, record.getStatus(), tStart, tComp,
                    vStart, vComp, record.getRetryCount(), error, firstError, record.isSourceDeleted()};
        } else {
            return new Object[]{record.getSourceId(), target, record.isDirectory(),
                    record.getSize(), mtime, record.getStatus(), tStart, tComp,
                    vStart, vComp, record.getRetryCount(), error, record.isSourceDeleted()};
        }
    }
}
