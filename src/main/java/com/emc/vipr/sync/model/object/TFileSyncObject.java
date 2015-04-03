package com.emc.vipr.sync.model.object;

import com.emc.vipr.sync.SyncPlugin;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileInputStream;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class TFileSyncObject extends FileSyncObject {
    public TFileSyncObject(SyncPlugin parentPlugin, MimetypesFileTypeMap mimeMap, File sourceFile, String relativePath) {
        super(parentPlugin, mimeMap, sourceFile, relativePath);
    }

    @Override
    protected InputStream createInputStream(File f) throws IOException {
        return new TFileInputStream(f);
    }

    @Override
    protected File createFile(String path) {
        return new TFile(path);
    }
}
