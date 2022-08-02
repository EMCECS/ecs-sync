/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.util;

import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;

import java.util.Collection;

public class VerifyUtil {
    public static void verifyObjects(TestStorage source, Collection<TestStorage.TestSyncObject> sourceObjects,
                                     TestStorage target, Collection<TestStorage.TestSyncObject> targetObjects,
                                     boolean verifyAcl) {
        for (TestStorage.TestSyncObject sourceObject : sourceObjects) {
            String currentPath = sourceObject.getRelativePath();
            Assertions.assertTrue(targetObjects.contains(sourceObject), currentPath + " - missing from target");
            for (TestStorage.TestSyncObject targetObject : targetObjects) {
                if (sourceObject.getRelativePath().equals(targetObject.getRelativePath())) {
                    verifyMetadata(sourceObject.getMetadata(), targetObject.getMetadata(), currentPath);
                    if (verifyAcl) verifyAcl(sourceObject.getAcl(), targetObject.getAcl());
                    if (sourceObject.getMetadata().isDirectory()) {
                        Assertions.assertTrue(targetObject.getMetadata().isDirectory(), currentPath + " - source is directory but target is not");
                        verifyObjects(source, source.getChildren(source.getIdentifier(sourceObject.getRelativePath(), true)),
                                target, target.getChildren(target.getIdentifier(targetObject.getRelativePath(), true)), verifyAcl);
                    } else {
                        Assertions.assertFalse(targetObject.getMetadata().isDirectory(), currentPath + " - source is data object but target is not");
                        Assertions.assertEquals(sourceObject.getMetadata().getContentType(), targetObject.getMetadata().getContentType(),
                                currentPath + " - content-type different");
                        Assertions.assertEquals(sourceObject.getMetadata().getContentLength(), targetObject.getMetadata().getContentLength(),
                                currentPath + " - data size different");
                        Assertions.assertArrayEquals(sourceObject.getData(), targetObject.getData(), currentPath + " - data not equal");
                    }
                }
            }
        }
    }

    private static void verifyMetadata(ObjectMetadata sourceMetadata, ObjectMetadata targetMetadata, String path) {
        if (sourceMetadata == null || targetMetadata == null)
            Assertions.fail(String.format("%s - metadata can never be null (source: %s, target: %s)",
                    path, sourceMetadata, targetMetadata));

        // must be reasonable about mtime; we can't always set it on the target
        if (sourceMetadata.getModificationTime() == null)
            Assertions.assertNull(targetMetadata.getModificationTime(), path + " - source mtime is null, but target is not");
        else if (targetMetadata.getModificationTime() == null)
            Assertions.fail(path + " - target mtime is null, but source is not");
        else
            Assertions.assertTrue(sourceMetadata.getModificationTime().compareTo(targetMetadata.getModificationTime()) < 1000,
                    path + " - target mtime is older");
        Assertions.assertEquals(sourceMetadata.getUserMetadata().size(), targetMetadata.getUserMetadata().size(),
                path + " - different user metadata count");
        for (String key : sourceMetadata.getUserMetadata().keySet()) {
            Assertions.assertEquals(sourceMetadata.getUserMetadataValue(key).trim(), targetMetadata.getUserMetadataValue(key).trim(),
                    path + " - meta[" + key + "] different"); // some systems trim metadata values
        }

        // not verifying system metadata here
    }

    private static void verifyAcl(ObjectAcl sourceAcl, ObjectAcl targetAcl) {
        // only verify ACL if it's set on the source
        if (sourceAcl != null) {
            Assertions.assertNotNull(targetAcl);
            Assertions.assertEquals(sourceAcl, targetAcl); // ObjectAcl implements .equals()
        }
    }
}
