/*
 * Copyright 2013-2015 EMC Corporation. All Rights Reserved.
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

package com.emc.ecs.sync.target;

import com.emc.ecs.sync.CommonOptions;
import com.emc.ecs.sync.filter.SyncFilter;
import com.emc.ecs.sync.model.SyncMetadata;
import com.emc.ecs.sync.model.object.SyncObject;
import com.emc.ecs.sync.model.object.TFileSyncObject;
import com.emc.ecs.sync.source.SyncSource;
import com.emc.ecs.sync.util.ConfigurationException;
import net.java.truevfs.access.TFile;
import net.java.truevfs.access.TFileOutputStream;
import org.apache.commons.cli.CommandLine;
import org.springframework.util.Assert;

import javax.activation.MimetypesFileTypeMap;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

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
    public boolean canHandleTarget(String targetUri) {
        return targetUri.startsWith(TARGET_PREFIX);
    }

    @Override
    public void parseCustomOptions(CommandLine line) {
        if (!targetUri.startsWith(TARGET_PREFIX))
            throw new ConfigurationException("target must start with " + TARGET_PREFIX);

        try {
            targetRoot = new TFile(new URI(targetUri.substring(TARGET_PREFIX.length())));
        } catch (URISyntaxException e) {
            throw new ConfigurationException("Invalid URI", e);
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        Assert.notNull(targetRoot, "you must specify a target archive");

        if (targetRoot.exists() && (!((TFile) targetRoot).isArchive() || !targetRoot.isDirectory()))
            throw new ConfigurationException("The target " + targetRoot + " exists and is not a valid archive. "
                    + "Note: tar files must fit entirely into memory and you will get this error if they are too large");

        if (monitorPerformance) {
            readPerformanceCounter = defaultPerformanceWindow();
            writePerformanceCounter = defaultPerformanceWindow();
        }
    }

    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        File destFile = createFile(targetRoot.getPath(), obj.getRelativePath());
        obj.setTargetIdentifier(destFile.getPath());
        return new TFileSyncObject(this, new MimetypesFileTypeMap(), createFile(targetRoot.getPath(), obj.getRelativePath()),
                obj.getRelativePath());
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
