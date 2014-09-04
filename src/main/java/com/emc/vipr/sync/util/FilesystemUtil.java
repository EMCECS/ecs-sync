package com.emc.vipr.sync.util;

import java.io.File;

public final class FilesystemUtil {
    public static final String DIR_DATA_FILE = ".viprdata";

    public static String getDirDataPath(String dirPath) {
        return new File(dirPath, DIR_DATA_FILE).getPath();
    }

    private FilesystemUtil() {
    }
}
