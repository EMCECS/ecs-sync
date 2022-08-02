/*
 * Copyright (c) 2022 Dell Inc. or its subsidiaries. All Rights Reserved.
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
package com.emc.ecs.sync.storage.s3;

import com.emc.ecs.sync.SkipObjectException;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.ObjectMetadata;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.TestStorage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.emc.ecs.sync.storage.s3.AbstractS3Storage.*;

public class AbstractS3Test {
    @Test
    public void testSkipIfExists() {
        Date newMtime = new Date();
        Date oldMtime = new Date(newMtime.getTime() - (10 * 3_600_000)); // 10 hours ago
        String normalEtag = "27dea2b9e19ee1533e1c6fa6288d9511";
        String mpuEtag = "b6715f06334ba04ee628ef1acc11ba25-150";
        String otherEtag = "3d56d1b48c2085c787457e7013beafb8";
        long normalSize = 1024, mpuSize = 2451212548L, otherSize = 10_000_000;

        // construct 2 test object contexts, 1 with MPU ETag, 1 with regular ETag
        TestStorage testStorage = new TestStorage();
        ObjectMetadata normalMetadata = new ObjectMetadata().withModificationTime(newMtime).withContentLength(normalSize).withHttpEtag(normalEtag);
        ObjectContext normalContext = new ObjectContext().withObject(new SyncObject(testStorage, "normal", normalMetadata));
        ObjectMetadata mpuMetadata = new ObjectMetadata().withModificationTime(newMtime).withContentLength(mpuSize).withHttpEtag(mpuEtag);
        ObjectContext mpuContext = new ObjectContext().withObject(new SyncObject(testStorage, "mpu", mpuMetadata));
        List<ObjectContext> contexts = Arrays.asList(normalContext, mpuContext);

        // this is only to have a reference imlementation
        AbstractS3Storage<?> storage = new AwsS3Storage();

        // for each object context, call skipIfExists(), passing in variations of targetObject
        for (ObjectContext context : contexts) {

            // 1. target has no source markers (x-emc-source-*), same size, newer mtime
            //    expect: SkipObjectException
            ObjectMetadata targetMetadata = new ObjectMetadata();
            targetMetadata.setContentLength(context.getObject().getMetadata().getContentLength());
            targetMetadata.setModificationTime(newMtime);
            try {
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.fail("legacy check with same size and newer mtime should throw SkipObjectException");
            } catch (SkipObjectException ignored) {
            }

            // 2. target has no source markers (x-emc-source-*), different size, newer mtime
            //    expect: no exception, no property flags
            targetMetadata = new ObjectMetadata();
            targetMetadata.setContentLength(otherSize);
            targetMetadata.setModificationTime(newMtime);
            storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
            Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

            // 3. target has no source markers (x-emc-source-*), same size, older mtime
            //    expect: no exception, no property flags
            targetMetadata = new ObjectMetadata();
            targetMetadata.setContentLength(context.getObject().getMetadata().getContentLength());
            targetMetadata.setModificationTime(oldMtime);
            storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
            Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

            // 4. target has no source markers (x-emc-source-*), different size, older mtime
            //    expect: no exception, no property flags
            targetMetadata = new ObjectMetadata();
            targetMetadata.setContentLength(otherSize);
            targetMetadata.setModificationTime(oldMtime);
            storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
            Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

            // 5. target has source markers, different source-etag, older source-mtime
            //    expect: no exception, no property flags
            targetMetadata = new ObjectMetadata();
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(oldMtime.getTime()));
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, otherEtag);
            storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
            Assertions.assertNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));

            // 6. target has source markers, same source-etag, older source-mtime
            //    expect: no exception, sourceEtagMatches flag is true
            targetMetadata = new ObjectMetadata();
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(oldMtime.getTime()));
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, context.getObject().getMetadata().getHttpEtag());
            storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
            Assertions.assertNotNull(context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));
            Assertions.assertTrue((Boolean) context.getObject().getProperty(PROP_SOURCE_ETAG_MATCHES));
            // reset objectContext state
            context.getObject().removeProperty(PROP_SOURCE_ETAG_MATCHES);

            // 7. target has source markers, different source-etag, newer source-mtime
            //    expect: SkipObjectException
            targetMetadata = new ObjectMetadata();
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(newMtime.getTime()));
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, otherEtag);
            try {
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.fail("newer source-mtime should throw SkipObjectException");
            } catch (SkipObjectException ignored) {
            }

            // 8. target has source markers, same source-etag, newer source-mtime
            //    expect: SkipObjectException
            targetMetadata = new ObjectMetadata();
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_MTIME, String.valueOf(newMtime.getTime()));
            targetMetadata.setUserMetadataValue(UMD_KEY_SOURCE_ETAG, context.getObject().getMetadata().getHttpEtag());
            try {
                storage.skipIfExists(context, new SyncObject(testStorage, "irrelevant", targetMetadata));
                Assertions.fail("newer source-mtime should throw SkipObjectException");
            } catch (SkipObjectException ignored) {
            }
        }
    }
}
