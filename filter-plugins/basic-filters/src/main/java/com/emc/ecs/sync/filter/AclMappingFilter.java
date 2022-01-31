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
package com.emc.ecs.sync.filter;

import com.emc.ecs.sync.config.ConfigurationException;
import com.emc.ecs.sync.config.filter.AclMappingConfig;
import com.emc.ecs.sync.model.ObjectAcl;
import com.emc.ecs.sync.model.ObjectContext;
import com.emc.ecs.sync.model.SyncObject;
import com.emc.ecs.sync.storage.SyncStorage;
import com.emc.ecs.sync.util.LineIterator;
import com.emc.ecs.sync.util.MultiValueMap;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AclMappingFilter extends AbstractFilter<AclMappingConfig> {
    private static final String GROUP = "group";
    private static final String USER = "user";
    private static final String PERMISSION = "permission";

    static final String MAP_PATTERN = "^(user|group|permission[1-9]?)\\.(.+?[^\\\\])=(.*)$";
    private static final String GRANT_PATTERN = "^(user|group)\\.(.+?[^\\\\])=(.*)$";

    private Map<String, String> userMap = new LinkedHashMap<>();
    private Map<String, String> groupMap = new LinkedHashMap<>();
    private MultiValueMap<String, String> permissionsMap = new MultiValueMap<>();
    private Map<String, Map<String, String>> permissionGroups = new TreeMap<>();
    private MultiValueMap<String, String> groupGrantsToAdd = new MultiValueMap<>();
    private MultiValueMap<String, String> userGrantsToAdd = new MultiValueMap<>();

    @Override
    public void configure(SyncStorage<?> source, Iterator<? extends SyncFilter<?>> filters, SyncStorage<?> target) {
        super.configure(source, filters, target);

        if (!options.isSyncAcl())
            throw new ConfigurationException("you must sync ACLs in the sync to use this plugin (--sync-acl)");

        if (config.getAclAddGrants() != null) {
            Pattern pattern = Pattern.compile(GRANT_PATTERN);
            for (String grant : config.getAclAddGrants()) {
                Matcher m = pattern.matcher(grant);

                if (!m.matches()) throw new ConfigurationException("could not parse add-grants option");

                if (m.group(1).trim().toLowerCase().equals(GROUP))
                    groupGrantsToAdd.add(m.group(2).trim(), m.group(3).trim());
                else if (m.group(1).trim().toLowerCase().equals(USER))
                    userGrantsToAdd.add(m.group(2).trim(), m.group(3).trim());
                else
                    throw new ConfigurationException("can only add user or group grants");
            }
        }

        // parse instructions
        if (config.getAclMapInstructions() != null || config.getAclMapFile() != null) {
            try {
                Pattern pattern = Pattern.compile(MAP_PATTERN);
                LineIterator i;
                if (config.getAclMapFile() != null)
                    i = new LineIterator(config.getAclMapFile());
                else
                    i = new LineIterator(new ByteArrayInputStream(config.getAclMapInstructions().getBytes(StandardCharsets.UTF_8)));
                while (i.hasNext()) {
                    Matcher m = pattern.matcher(i.next());
                    if (!m.matches())
                        throw new ConfigurationException("parse error in mapping file on line " + i.getCurrentLine());
                    String key = m.group(1).trim().toLowerCase();
                    if (key.equals(USER))
                        userMap.put(m.group(2).trim(), m.group(3).trim());
                    else if (key.equals(GROUP))
                        groupMap.put(m.group(2).trim(), m.group(3).trim());
                    else if (key.equals(PERMISSION)) {
                        for (String permission : m.group(3).split(",")) {
                            permissionsMap.add(m.group(2).trim(), permission.trim());
                        }
                    } else if (key.startsWith(PERMISSION)) {
                        String groupKey = key.substring(PERMISSION.length());
                        Map<String, String> groupPermissionMap = permissionGroups.get(groupKey);
                        if (groupPermissionMap == null) {
                            groupPermissionMap = new LinkedHashMap<>();
                            permissionGroups.put(groupKey, groupPermissionMap);
                        }
                        groupPermissionMap.put(m.group(2).trim(), m.group(3).trim());
                    } else throw new RuntimeException("can only map user, group or permission");
                }
            } catch (Exception e) {
                throw new ConfigurationException("could not parse mapping instructions", e);
            }
        }

        if (config.getAclAppendDomain() != null && !config.getAclAppendDomain().startsWith("@"))
            config.setAclAppendDomain("@" + config.getAclAppendDomain());
    }

    @Override
    public void filter(ObjectContext objectContext) {
        SyncObject object = objectContext.getObject();
        ObjectAcl acl = object.getAcl();
        if (acl == null) {
            acl = new ObjectAcl();
            object.setAcl(acl);
        }

        // map owner
        if (config.isAclStripUsers()) acl.setOwner(null);
        else acl.setOwner(getTargetUser(acl.getOwner()));

        // pare-down filters
        Set<String> userPermGroupMatches = new HashSet<>(), groupPermGroupMatches = new HashSet<>();

        // map users
        Map<String, SortedSet<String>> userGrants = new TreeMap<>();
        userGrants.putAll(acl.getUserGrants());
        acl.getUserGrants().clear();
        if (!config.isAclStripUsers()) {
            for (String sourceUser : userGrants.keySet()) {
                for (String sourcePermission : userGrants.get(sourceUser)) {

                    // get mapped user
                    String targetUser = getTargetUser(sourceUser);

                    // null user means remove them (don't re-add them to the grants), short-circuit
                    if (targetUser == null) continue;

                    // get mapped permissions in target system
                    String[] targetPermissions = getTargetPermissions(sourceUser, sourcePermission, userPermGroupMatches);

                    // if they aren't specified in the mapping file, don't change them
                    if (targetPermissions == null) targetPermissions = new String[]{sourcePermission};

                    // add grants for target (empty array means grants were removed or pared-down)
                    for (String targetPermission : targetPermissions) {
                        acl.addUserGrant(targetUser, targetPermission);
                    }
                }
            }

            // add users
            for (String targetUser : userGrantsToAdd.keySet()) {
                for (String permission : userGrantsToAdd.get(targetUser)) {
                    acl.addUserGrant(targetUser, permission);
                }
            }
        } // !dropUsers

        // map groups
        Map<String, SortedSet<String>> groupGrants = new TreeMap<>();
        groupGrants.putAll(acl.getGroupGrants());
        acl.getGroupGrants().clear();
        if (!config.isAclStripGroups()) {
            for (String sourceGroup : groupGrants.keySet()) {
                for (String sourcePermission : groupGrants.get(sourceGroup)) {

                    // get mapped group
                    String targetGroup = groupMap.get(sourceGroup);
                    if (targetGroup == null) targetGroup = sourceGroup; // not mapped; don't change

                    // empty group name means remove them (don't re-add them to the grants); short-circuit
                    if (targetGroup.isEmpty()) continue;

                    // get mapped permissions in target system
                    String[] targetPermissions = getTargetPermissions(sourceGroup, sourcePermission, groupPermGroupMatches);

                    // if they aren't specified in the mapping file, don't change them
                    if (targetPermissions == null) targetPermissions = new String[]{sourcePermission};

                    // add grants for target (empty array means grants were removed or pared-down)
                    for (String targetPermission : targetPermissions) {
                        acl.addGroupGrant(targetGroup, targetPermission);
                    }
                }
            }

            // add groups
            for (String targetGroup : groupGrantsToAdd.keySet()) {
                for (String permission : userGrantsToAdd.get(targetGroup)) {
                    acl.addGroupGrant(targetGroup, permission);
                }
            }
        } // !dropGroups

        getNext().filter(objectContext);
    }

    // TODO: if verification ever includes ACLs, reverse the ACL map here
    @Override
    public SyncObject reverseFilter(ObjectContext objectContext) {
        return getNext().reverseFilter(objectContext);
    }

    private String getTargetUser(String sourceUser) {
        String targetUser = userMap.get(sourceUser);

        // if not mapped, keep it the same
        if (targetUser == null) targetUser = sourceUser;

        // if user is mapped to "" (empty), that signifies we should remove them
        if (targetUser == null || targetUser.isEmpty()) return null;

        // strip domain if required
        if (config.isAclStripDomain()) targetUser = targetUser.replaceAll("@.*$", "");

        // append domain if required
        if (config.getAclAppendDomain() != null) targetUser += config.getAclAppendDomain();

        return targetUser;
    }

    private String[] getTargetPermissions(String sourceName, String sourcePermission, Set<String> groupMatches) {
        List<String> targetPermissions = new ArrayList<>();
        boolean mappingFound = false;

        // process groups first
        for (String groupKey : permissionGroups.keySet()) {

            // a permission can only match once per user per group
            String permission = permissionGroups.get(groupKey).get(sourcePermission);
            if (permission != null) {
                mappingFound = true;
                if (!groupMatches.contains(sourceName + ":" + groupKey)) {
                    targetPermissions.add(permission);
                    groupMatches.add(sourceName + ":" + groupKey); // remember that we've matched this user with this perm. group
                }
            }
        }

        if (!mappingFound) {
            List<String> permissions = permissionsMap.get(sourcePermission);
            if (permissions != null) {
                mappingFound = true;
                targetPermissions.addAll(permissions);
            }
        }

        if (mappingFound) return targetPermissions.toArray(new String[targetPermissions.size()]);

        else return null; // indicate that there is no mapping for the source permission
    }
}
