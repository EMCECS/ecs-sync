/*
 * Copyright 2013-2017 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.model.ObjectContext;
import org.springframework.jdbc.core.JdbcTemplate;

public class NoDbService extends AbstractDbService {
    @Override
    public void deleteDatabase() {
    }

    @Override
    protected JdbcTemplate createJdbcTemplate() {
        return null;
    }

    @Override
    protected void createTable() {
    }

    @Override
    public SyncRecord getSyncRecord(ObjectContext objectContext) {
        return null;
    }

    @Override
    public boolean setStatus(ObjectContext objectContext, String error, boolean newRow) {
        return true;
    }

    @Override
    public boolean setDeleted(ObjectContext context, boolean newRow) {
        return true;
    }
}
