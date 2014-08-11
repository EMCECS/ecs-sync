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

package com.emc.atmos.sync.util;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

public final class ArchiveUtil {
    private ArchiveUtil() {
    }

    public static File parseSourceOption(String sourceOption) throws URISyntaxException {
        sourceOption = sourceOption.substring(8); // remove archive:
        if (sourceOption.startsWith("//")) sourceOption = sourceOption.substring(2); // in case archive:// was used
        URI sourceUri = new URI(sourceOption);
        return sourceUri.isAbsolute() ? new File(sourceUri) : new File(sourceOption);
    }
}
