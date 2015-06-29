package com.emc.vipr.sync.model.object;

import com.emc.vipr.sync.SyncPlugin;
import com.emc.vipr.sync.model.Checksum;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.S3Util;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.StoredObject;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

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
        if (isDirectory()) return;
        StoredObject swiftMetaData=swift.getContainer(containerName).getObject(key);
    }

    @Override
    protected InputStream createSourceInputStream() {


        if (isDirectory()) return null;
        return new BufferedInputStream(swift.getContainer(containerName).getObject(key).downloadObjectAsInputStream(), parentPlugin.getBufferSize());
    }


    protected SyncMetadata toSyncMeta(StoredObject swiftMetaData) {
        SyncMetadata meta = new SyncMetadata();

        //meta.setCacheControl(swiftMetaData.getc);
       // meta.setContentDisposition(s3meta.getContentDisposition());
      //  meta.setContentEncoding(s3meta.getContentEncoding());
        if (swiftMetaData.getEtag() != null) meta.setChecksum(new Checksum("MD5", swiftMetaData.getEtag()));
        meta.setContentType(swiftMetaData.getContentType());
      //  meta.setHttpExpires(s3meta.getHttpExpiresDate());
        meta.setExpirationDate(swiftMetaData.getDeleteAtAsDate());
        meta.setModificationTime(swiftMetaData.getLastModifiedAsDate());
        meta.setSize(swiftMetaData.getContentLength());
        Map<String, Object> metdata = swiftMetaData.getMetadata();
        meta.setUserMetadata(toMetaMap(metdata));

        return meta;
    }


    protected Map<String, SyncMetadata.UserMetadata> toMetaMap(Map<String, Object> sourceMap) {
        Map<String, SyncMetadata.UserMetadata> metaMap = new HashMap<String, SyncMetadata.UserMetadata>();
        for (String key : sourceMap.keySet()) {
            metaMap.put(key, new SyncMetadata.UserMetadata(key, (String)sourceMap.get(key)));
        }
        return metaMap;
    }

    public String getContainerNameName() {
        return containerName;
    }

    public String getKey() {
        return key;
    }
}
