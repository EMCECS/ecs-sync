/*
 * Copyright 2013-2018 EMC Corporation. All Rights Reserved.
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
package sync.ui.migration.a2e


import sync.ui.migration.AbstractMigrationTask

abstract class AbstractS3ProxyTask extends AbstractMigrationTask<A2EMigration> {
    static subtPreMigKey = 'subtenants-pre-migration'
    static subtMigKey = 'subtenants-migrating'
    static subtPostMigKey = 'subtenants-post-migration'

    static userMapKey(subtenantId) { "${subtenantId}/source-to-target-user-mapping" }

    static bucketsPreMigKey(subtenantId) { "${subtenantId}/buckets-pre-migration" }

    static bucketsMigKey(subtenantId) { "${subtenantId}/buckets-migrating" }
}
