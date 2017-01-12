package com.emc.ecs.sync.rest;

import com.emc.ecs.sync.service.SyncRecord;

import java.io.IOException;

public class DbDumpWriter extends AbstractCsvWriter<SyncRecord> {
    public DbDumpWriter(Iterable<SyncRecord> records) throws IOException {
        super(records);
    }

    @Override
    protected String[] getHeaders() {
        return new String[]{"Source ID", "Target ID", "Directory", "Size", "Source mtime", "Status",
                "Transfer Start", "Transfer Complete", "Verify Start", "Verify Complete",
                "Retry Count", "Error Message", "Source Deleted"};
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
        return new Object[]{record.getSourceId(), target, record.isDirectory(), record.getSize(), mtime, record.getStatus(),
                tStart, tComp, vStart, vComp, record.getRetryCount(), error, record.isSourceDeleted()};
    }
}
