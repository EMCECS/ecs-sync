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

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class SyncAcl implements Cloneable {
    String owner;
    TreeMap<String, SortedSet<String>> userGrants = new TreeMap<String, SortedSet<String>>();
    TreeMap<String, SortedSet<String>> groupGrants = new TreeMap<String, SortedSet<String>>();

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public synchronized void addUserGrant(String user, String permission) {
        SortedSet<String> permissions = userGrants.get(user);
        if (permissions == null) {
            permissions = new TreeSet<String>();
            userGrants.put(user, permissions);
        }
        permissions.add(permission);
    }

    public synchronized void removeUserGrant(String user, String permission) {
        SortedSet<String> permissions = userGrants.get(user);
        if (permissions != null) {
            permissions.remove(permission);
            if (permissions.isEmpty()) userGrants.remove(user);
        }
    }

    public Map<String, SortedSet<String>> getUserGrants() {
        return userGrants;
    }

    public synchronized void addGroupGrant(String group, String permission) {
        SortedSet<String> permissions = groupGrants.get(group);
        if (permissions == null) {
            permissions = new TreeSet<String>();
            groupGrants.put(group, permissions);
        }
        permissions.add(permission);
    }

    public synchronized void removeGroupGrant(String group, String permission) {
        SortedSet<String> permissions = groupGrants.get(group);
        if (permissions != null) {
            permissions.remove(permission);
            if (permissions.isEmpty()) groupGrants.remove(group);
        }
    }

    public Map<String, SortedSet<String>> getGroupGrants() {
        return groupGrants;
    }

    /**
     * content-based equality (no identity here)
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SyncAcl)) return false;

        SyncAcl syncAcl = (SyncAcl) o;

        if (owner != null ? !owner.equals(syncAcl.owner) : syncAcl.owner != null) return false;
        if (groupGrants != null ? !groupGrants.equals(syncAcl.groupGrants) : syncAcl.groupGrants != null) return false;
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

        clone.userGrants = (TreeMap<String, SortedSet<String>>) userGrants.clone();
        // make sure value lists are cloned
        for (Map.Entry<String, SortedSet<String>> entry : clone.userGrants.entrySet()) {
            SortedSet<String> copiedValue = new TreeSet<String>();
            copiedValue.addAll(entry.getValue());
            entry.setValue(copiedValue);
        }

        clone.groupGrants = (TreeMap<String, SortedSet<String>>) groupGrants.clone();
        // make sure value lists are cloned
        for (Map.Entry<String, SortedSet<String>> entry : clone.groupGrants.entrySet()) {
            SortedSet<String> copiedValue = new TreeSet<String>();
            copiedValue.addAll(entry.getValue());
            entry.setValue(copiedValue);
        }

        return clone;
    }

    @Override
    public String toString() {
        return "SyncAcl {owner='" + owner + "', userGrants=" + userGrants + ", groupGrants=" + groupGrants + '}';
    }
}
