package com.emc.ecs.sync.model.object;

import com.emc.ecs.sync.util.ClamAvInputStream;

import java.io.InputStream;

/**
 * SyncObject wrapper that will scan the bytestream for viruses using ClamAV.
 */
public class ClamAvSyncObject extends WrappedSyncObject {
    private String host;
    private int port;
    private long maxScanSize;

    /**
     * Creates a new ClamAvSyncObject.
     * @param parent the SyncObject to wrap
     * @param host the host running clamd
     * @param port the port clamd is running on
     * @param maxScanSize the maximum number of bytes clamd will accept.
     */
    public ClamAvSyncObject(SyncObject parent, String host, int port, long maxScanSize) {
        super(parent);
        this.host = host;
        this.port = port;
        this.maxScanSize = maxScanSize;
    }

    @Override
    public InputStream getInputStream() {
        return new ClamAvInputStream(parent.getInputStream(), host, port, maxScanSize);
    }
}
