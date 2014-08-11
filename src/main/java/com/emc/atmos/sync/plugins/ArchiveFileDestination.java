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

import com.emc.atmos.sync.util.ArchiveUtil;
import com.emc.atmos.sync.util.AtmosMetadata;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileOutputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;

public class ArchiveFileDestination extends FilesystemDestination {
    private static final String DEST_PREFIX = "archive:";

    @Override
    protected File getFile(String rootPath, String relativePath) {
        return new TFile(new File(rootPath, relativePath));
    }

    @Override
    protected OutputStream getFileStream(File file) throws IOException {
        return new TFileOutputStream(file);
    }

    @SuppressWarnings("static-access")
    @Override
    public Options getOptions() {
        Options opts = new Options();

        opts.addOption(OptionBuilder.withDescription(NO_META_DESC)
                .withLongOpt(NO_META_OPT).create());

        return opts;
    }

    @Override
    public boolean parseOptions(CommandLine line) {
        super.parseOptions(line);

        String destOption = line.getOptionValue(CommonOptions.DESTINATION_OPTION);
        if (destOption != null && destOption.startsWith(DEST_PREFIX)) {
            File destFile;
            try {
                destFile = ArchiveUtil.parseSourceOption(destOption);
            } catch (URISyntaxException e) {
                throw new RuntimeException("Failed to parse URI: " + destOption + ": " + e.getMessage(), e);
            }
            destination = new TFile(destFile);
            if (destination.exists() && (!((TFile) destination).isArchive() || !destination.isDirectory()))
                throw new IllegalArgumentException("The destination " + destination + " exists and is not a valid archive. "
                        + "Note: tar files must fit entirely into memory and you will get this error if they are too large");

            if (line.hasOption(NO_META_OPT)) {
                noMetadata = true;
            }

            if (line.hasOption(CommonOptions.FORCE_OPTION)) {
                force = true;
            }

            return true;
        }

        return false;
    }

    @Override
    public String getName() {
        return "Archive File Destination";
    }

    @Override
    public String getDocumentation() {
        return "The archive file source writes data to an archive file (tar, zip, etc.)  " +
                "It is triggered by setting the destination to a valid archive URL:\n" +
                "archive:[<scheme>://]<path>, e.g. archive:file:///home/user/myfiles.tar\n" +
                "or archive:http://company.com/bundles/project.tar.gz or archive:cwd_file.zip\n" +
                "Objects will be transferred as the contents of the archive. " +
                "By default, Atmos metadata will be stored in " +
                AtmosMetadata.META_DIR + " directories with corresponding file names; " +
                "use --" + NO_META_OPT + " to exclude metadata from transfer. " +
                "If the destination archive exists, its contents will be updated selectively based on " +
                "modification timestamps. To force the transfer of all objects, use the --" +
                CommonOptions.FORCE_OPTION + " option.";
    }
}
