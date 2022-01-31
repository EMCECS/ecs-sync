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
package com.emc.ecs.sync.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ObjectAclTest {
    @Test
    public void testJsonSerialization() {
        ObjectAcl acl = new ObjectAcl();
        acl.addGroupGrant("other", "read");
        acl.addUserGrant("stu", "full-control");
        acl.addUserGrant("jason", "none");

        String jsonString = acl.toJson();

        ObjectAcl aclFromJson = ObjectAcl.fromJson(jsonString);

        Assertions.assertEquals(acl, aclFromJson);
    }
}
