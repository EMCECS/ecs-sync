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
package com.emc.atmos.sync.plugins;

import com.emc.atmos.sync.util.AtmosMetadata;
import com.emc.atmos.sync.util.CountingInputStream;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class ArchiveFileSource extends FilesystemSource {
    private static final Logger l4j = Logger.getLogger(ArchiveFileSource.class);

    private static final String SOURCE_PREFIX = "archive:";

    public ArchiveFileSource() {
        super();
        recursive = true;
        delete = false;
        useAbsolutePath = false;
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        Options opts = new Options();
        opts.addOption(OptionBuilder.withDescription(IGNORE_META_DESC)
                .withLongOpt(IGNORE_META_OPT).create());
        addOptions(opts);

        return opts;
    }

    @Override
    public boolean parseOptions(CommandLine line) {
        super.parseOptions(line);

        String sourceOption = line.getOptionValue(CommonOptions.SOURCE_OPTION);
        if (sourceOption == null) {
            return false;
        }
        if (sourceOption.startsWith(SOURCE_PREFIX)) {
            File sourceFile;
            try {
                sourceFile = parseSourceOption(sourceOption);
            } catch (URISyntaxException e) {
                throw new RuntimeException("Failed to parse URI: " + sourceOption + ": " + e.getMessage(), e);
            }
            source = new TFile(sourceFile);
            if (!source.exists())
                throw new IllegalArgumentException("The source " + source + " does not exist");
            if (!((TFile) source).isArchive() || !source.isDirectory())
                throw new IllegalArgumentException("The source " + source + " is not a valid archive. "
                        + "Note: tar files must fit entirely into memory and you will get this error if they are too large");

            if (!recursive) {
                l4j.info("Archive sources will always use recursive processing");
            }
            if (delete) {
                l4j.warn("Archive sources cannot use the delete option!");
            }
            recursive = true;
            delete = false;

            if (line.hasOption(CommonOptions.IO_BUFFER_SIZE_OPTION)) {
                bufferSize = Integer.parseInt(line.getOptionValue(CommonOptions.IO_BUFFER_SIZE_OPTION));
            }

            return true;
        }
        return false;
    }

    /**
     * @see com.emc.atmos.sync.plugins.SyncPlugin#getName()
     */
    @Override
    public String getName() {
        return "Archive File Source";
    }

    /**
     * @see com.emc.atmos.sync.plugins.SyncPlugin#getDocumentation()
     */
    @Override
    public String getDocumentation() {
        return "The archivefile source reads data from an archive file (tar, zip, etc.)  " +
                "It is triggered by setting the source to a valid archive URL:\n" +
                "archive:[<scheme>://]<path>, e.g. archive:file:///home/user/myfiles.tar\n" +
                "or archive:http://company.com/bundles/project.tar.gz or archive:cwd_file.zip\n" +
                "The contents of " +
                "the archive will be transferred.  By default, any Atmos metadata files inside " +
                AtmosMetadata.META_DIR + " directories will be assigned to their " +
                "corresponding files; use --" + IGNORE_META_OPT +
                " to ignore the metadata directory.";
    }

    @Override
    public void setUseAbsolutePath(boolean useAbsolutePath) {
        if (useAbsolutePath) l4j.warn("Archive sources will always use a relative path!");
    }

    @Override
    protected FileSyncObject createFileSyncObject(File f) {
        return new TFileSyncObject(f);
    }

    protected File parseSourceOption(String sourceOption) throws URISyntaxException {
        sourceOption = sourceOption.substring(8); // remove archive:
        if (sourceOption.startsWith("//")) sourceOption = sourceOption.substring(2); // in case archive:// was used
        URI sourceUri = new URI(sourceOption);
        return sourceUri.isAbsolute() ? new File(sourceUri) : new File(sourceOption);
    }

    public class TFileSyncObject extends FileSyncObject {
        public TFileSyncObject(File f) {
            super(f);
        }

        @Override
        public synchronized InputStream getInputStream() {
            if (f.isDirectory()) {
                return null;
            }
            if (in == null) {
                try {
                    in = new CountingInputStream(new BufferedInputStream(new TFileInputStream(f), bufferSize));
                } catch (IOException e) {
                    throw new RuntimeException("Could not open file:" + f, e);
                }
            }

            return in;
        }
    }
}
