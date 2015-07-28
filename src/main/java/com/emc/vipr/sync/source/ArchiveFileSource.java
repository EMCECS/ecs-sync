/*
 * Copyright 2014 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.source;

import com.emc.vipr.sync.CommonOptions;
import com.emc.vipr.sync.filter.SyncFilter;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.object.FileSyncObject;
import com.emc.vipr.sync.model.object.TFileSyncObject;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import net.java.truevfs.access.TFile;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;

public class ArchiveFileSource extends FilesystemSource {
    private static final Logger l4j = Logger.getLogger(ArchiveFileSource.class);

    private static final String SOURCE_PREFIX = "archive:";

    public ArchiveFileSource() {
        super();
        useAbsolutePath = false;
    }

    @Override
    public boolean canHandleSource(String sourceUri) {
        return sourceUri.startsWith(SOURCE_PREFIX);
    }

    @Override
    public Options getCustomOptions() {
        return new Options();
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        if (!sourceUri.startsWith(SOURCE_PREFIX))
            throw new ConfigurationException("source must start with " + SOURCE_PREFIX);

        try {
            rootFile = new TFile(new URI(sourceUri));
        } catch (URISyntaxException e) {
            throw new ConfigurationException("Invalid URI", e);
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        super.configure(source, filters, target);

        if (!((TFile) rootFile).isArchive() || !rootFile.isDirectory())
            throw new ConfigurationException("The source " + rootFile + " is not a valid archive. "
                    + "Note: tar files must fit entirely into memory and you will get this error if they are too large");
    }

    @Override
    public Iterator<FileSyncObject> iterator() {
        return Arrays.asList((FileSyncObject) new TFileSyncObject(this, mimeMap, rootFile, getRelativePath(rootFile))).iterator();
    }

    @Override
    public Iterator<FileSyncObject> childIterator(FileSyncObject syncObject) {
        if (syncObject.isDirectory())
            return new TDirectoryIterator(syncObject.getRawSourceIdentifier(), syncObject.getRelativePath());
        else
            return null;
    }

    /**
     * @see com.emc.vipr.sync.SyncPlugin#getName()
     */
    @Override
    public String getName() {
        return "Archive File Source";
    }

    /**
     * @see com.emc.vipr.sync.SyncPlugin#getDocumentation()
     */
    @Override
    public String getDocumentation() {
        return "The archivefile source reads data from an archive file (tar, zip, etc.)  " +
                "It is triggered by setting the source to a valid archive URL:\n" +
                "archive:[<scheme>://]<path>, e.g. archive:file:///home/user/myfiles.tar\n" +
                "or archive:http://company.com/bundles/project.tar.gz or archive:cwd_file.zip\n" +
                "The contents of " +
                "the archive will be transferred.  By default, any object metadata files inside " +
                SyncMetadata.METADATA_DIR + " directories will be assigned to their " +
                "corresponding files; use --" + CommonOptions.IGNORE_METADATA_OPTION +
                " to ignore the metadata directory.";
    }

    @Override
    public void setUseAbsolutePath(boolean useAbsolutePath) {
        if (useAbsolutePath) l4j.warn("Archive sources will always use a relative path!");
    }

    @Override
    public void delete(FileSyncObject syncObject) {
        // TODO: implement (low priority)
    }

    public class TDirectoryIterator extends DirectoryIterator {
        public TDirectoryIterator(File directory, String relativePath) {
            super(directory, relativePath);
        }

        @Override
        protected FileSyncObject getNextObject() {
            FileSyncObject next = super.getNextObject();
            if (next == null) return null;
            return new TFileSyncObject(ArchiveFileSource.this, mimeMap, next.getRawSourceIdentifier(), next.getRelativePath());
        }
    }
}
