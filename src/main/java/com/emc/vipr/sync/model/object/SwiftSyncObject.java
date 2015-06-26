package com.emc.vipr.sync.model.object;

import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.util.S3Util;
import org.javaswift.joss.model.Account;

import java.io.InputStream;

/**
 * Created by joshia7 on 6/26/15.
 */
public class SwiftSyncObject extends AbstractSyncObject<String> {

    private SyncPlugin parentPlugin;
    private final Account swift;
    private final String containerName;
    private String key;

    public SwiftSyncObject(SyncPlugin parentPlugin, Account swift, String containerName, String key, String relativePath, boolean isCommonPrefix) {
        super(S3Util.fullPath(containerName, key), S3Util.fullPath(containerName, key), relativePath, isCommonPrefix);
        this.parentPlugin = parentPlugin;
        this.swift = swift;
        this.containerName = containerName;
        this.key = key;
    }


    @Override
    protected void loadObject() {

    }

    @Override
    protected InputStream createSourceInputStream() {
        return null;
    }
}
