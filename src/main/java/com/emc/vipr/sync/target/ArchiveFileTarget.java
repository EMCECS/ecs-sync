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

package com.emc.vipr.sync.target;

import com.emc.vipr.sync.CommonOptions;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.util.ConfigurationException;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileOutputStream;
import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class ArchiveFileTarget extends FilesystemTarget {
    private static final String TARGET_PREFIX = "archive:";

    @Override
    protected File createFile(String rootPath, String relativePath) {
        return new TFile(new File(rootPath, relativePath));
    }

    @Override
    protected OutputStream createOutputStream(File file) throws IOException {
        return new TFileOutputStream(file);
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        if (!targetUri.startsWith(TARGET_PREFIX))
            throw new ConfigurationException("source must start with " + TARGET_PREFIX);

        try {
            targetRoot = new TFile(new URI(targetUri));
        } catch (URISyntaxException e) {
            throw new ConfigurationException("Invalid URI", e);
        }
        if (targetRoot.exists() && !((TFile) targetRoot).isArchive() || !targetRoot.isDirectory())
            throw new ConfigurationException("The target " + targetRoot + " exists and is not a valid archive. "
                    + "Note: tar files must fit entirely into memory and you will get this error if they are too large");
    }

    @Override
    public String getName() {
        return "Archive File Target";
    }

    @Override
    public String getDocumentation() {
        return "The archive file target writes data to an archive file (tar, zip, etc.)  " +
                "It is triggered by setting the target to a valid archive URL:\n" +
                "archive:[<scheme>://]<path>, e.g. archive:file:///home/user/myfiles.tar\n" +
                "or archive:http://company.com/bundles/project.tar.gz or archive:cwd_file.zip\n" +
                "Objects will be transferred as the contents of the archive. " +
                "By default, metadata will be stored in " +
                SyncMetadata.METADATA_DIR + " directories with corresponding file names; " +
                "use --" + CommonOptions.IGNORE_METADATA_OPTION + " to exclude metadata from transfer. " +
                "If the target archive exists, its contents will be updated selectively based on " +
                "modification timestamps. To force the transfer of all objects, use the --" +
                CommonOptions.FORCE_OPTION + " option.";
    }
}
