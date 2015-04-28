/*
 * Copyright 2015 EMC Corporation. All Rights Reserved.
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
package com.emc.vipr.sync.model;

import com.emc.vipr.sync.util.MultiValueMap;

public class SyncAcl implements Cloneable {
    String owner;
    MultiValueMap<String, String> userGrants = new MultiValueMap<String, String>();
    MultiValueMap<String, String> groupGrants = new MultiValueMap<String, String>();

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public MultiValueMap<String, String> getUserGrants() {
        return userGrants;
    }

    public void setUserGrants(MultiValueMap<String, String> userGrants) {
        this.userGrants = userGrants;
    }

    public MultiValueMap<String, String> getGroupGrants() {
        return groupGrants;
    }

    public void setGroupGrants(MultiValueMap<String, String> groupGrants) {
        this.groupGrants = groupGrants;
    }

    /**
     * content-based equality (no identity here)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SyncAcl)) return false;

        SyncAcl syncAcl = (SyncAcl) o;

        if (groupGrants != null ? !groupGrants.equals(syncAcl.groupGrants) : syncAcl.groupGrants != null) return false;
        if (owner != null ? !owner.equals(syncAcl.owner) : syncAcl.owner != null) return false;
        if (userGrants != null ? !userGrants.equals(syncAcl.userGrants) : syncAcl.userGrants != null) return false;

        return true;
    }

    /**
     * content-based equality (no identity here)
     */
    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + (userGrants != null ? userGrants.hashCode() : 0);
        result = 31 * result + (groupGrants != null ? groupGrants.hashCode() : 0);
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object clone() throws CloneNotSupportedException {
        SyncAcl clone = (SyncAcl) super.clone();
        clone.setUserGrants((MultiValueMap<String, String>) userGrants.clone());
        clone.setGroupGrants((MultiValueMap<String, String>) groupGrants.clone());
        return clone;
    }
}
