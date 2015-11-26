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
package com.emc.ecs.sync.service;

import com.emc.ecs.sync.model.ObjectStatus;
import com.emc.ecs.sync.model.object.SyncObject;
import org.springframework.jdbc.core.JdbcTemplate;

public class NoDbService extends DbService {
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
    public SyncRecord getSyncRecord(SyncObject object) {
        return null;
    }

    @Override
    public void setStatus(SyncObject object, ObjectStatus status, String error, boolean newRow) {
    }
}
