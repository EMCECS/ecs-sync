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
package com.emc.vipr.sync.filter;

import com.emc.vipr.sync.CommonOptions;
import com.emc.vipr.sync.model.SyncAcl;
import com.emc.vipr.sync.model.SyncMetadata;
import com.emc.vipr.sync.model.object.SyncObject;
import com.emc.vipr.sync.source.SyncSource;
import com.emc.vipr.sync.target.SyncTarget;
import com.emc.vipr.sync.util.ConfigurationException;
import com.emc.vipr.sync.util.FileLineIterator;
import com.emc.vipr.sync.util.MultiValueMap;
import com.emc.vipr.sync.util.OptionBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AclMappingFilter extends SyncFilter {
    private static final String GROUP = "group";
    private static final String USER = "user";
    private static final String PERMISSION = "permission";

    public static final String ACTIVATION_NAME = "acl-mapping";

    public static final String ACL_MAPPING_OPTION = "acl-map-file";
    public static final String ACL_MAPPING_DESC = "Specifies the file that contains the mapping of identities and permissions from source to target. Each entry is on a separate line and specifies a group/user/permission source and target name[s] like so:\n" +
            "group.<source_group>=<target_group>\n" +
            "user.<source_user>=<target_user>\n" +
            "permission.<source_perm>=<target_perm>[,<target_perm>..]\n" +
            "You can also pare down permissions that are redundant in the target system by using permission groups. I.e.:\n" +
            "permission1.WRITE=READ_WRITE\n" +
            "permission1.READ=READ\n" +
            "will pare down separate READ and WRITE permissions into one READ_WRITE/READ (note the ordering by priority). Groups are processed before straight mappings. Leave the target value blank to flag an identity/permission that should be removed (perhaps it does not exist in the target system).";
    public static final String ACL_MAPPING_ARG_NAME = "map-file";
    public static final String MAP_PATTERN = "^(user|group|permission[1-9]?)\\.(.+?[^\\\\])=(.*)$";

    public static final String APPEND_DOMAIN_OPTION = "append-domain";
    public static final String APPEND_DOMAIN_DESC = "Appends a directory realm/domain to each user that is mapped. Useful when mapping POSIX users to LDAP identities.";
    public static final String APPEND_DOMAIN_ARG_NAME = "LDAP-domain";

    public static final String STRIP_DOMAIN_OPTION = "strip-domain";
    public static final String STRIP_DOMAIN_DESC = "Strips the directory realm/domain from each user that is mapped. Useful when mapping LDAP identities to POSIX users.";

    public static final String ADD_GRANTS_OPTION = "add-grants";
    public static final String ADD_GRANTS_DESC = "Adds a comma-separated list of grants to all objects synced to the target system. Syntax is like so (repeats are allowed):\n" +
            "group.<target_group>=<target_perm>,user.<target_user>=<target_perm>";
    public static final String ADD_GRANTS_ARG_NAME = "grant-list";
    public static final String GRANT_PATTERN = "^(user|group)\\.(.+?[^\\\\])=(.*)$";

    public static final String DROP_USERS_OPTION = "drop-users";
    public static final String DROP_USERS_DESC = "Drops all users from each object's ACL. Use with --" + ADD_GRANTS_OPTION + " to add specific user grants instead.";

    public static final String DROP_GROUPS_OPTION = "drop-groups";
    public static final String DROP_GROUPS_DESC = "Drops all groups from each object's ACL. Use with --" + ADD_GRANTS_OPTION + " to add specific group grants instead.";

    private String aclMapFile;
    private String domainToAppend;
    private boolean stripDomain;
    private boolean dropUsers;
    private boolean dropGroups;

    private Map<String, String> userMap = new LinkedHashMap<String, String>();
    private Map<String, String> groupMap = new LinkedHashMap<String, String>();
    private MultiValueMap<String, String> permissionsMap = new MultiValueMap<String, String>();
    private Map<String, Map<String, String>> permissionGroups = new TreeMap<String, Map<String, String>>();
    private MultiValueMap<String, String> groupGrantsToAdd = new MultiValueMap<String, String>();
    private MultiValueMap<String, String> userGrantsToAdd = new MultiValueMap<String, String>();

    @Override
    public String getActivationName() {
        return ACTIVATION_NAME;
    }

    @Override
    public Options getCustomOptions() {
        Options opts = new Options();
        opts.addOption(new OptionBuilder().withLongOpt(ACL_MAPPING_OPTION).withDescription(ACL_MAPPING_DESC)
                .hasArg().withArgName(ACL_MAPPING_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(APPEND_DOMAIN_OPTION).withDescription(APPEND_DOMAIN_DESC)
                .hasArg().withArgName(APPEND_DOMAIN_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(STRIP_DOMAIN_OPTION).withDescription(STRIP_DOMAIN_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(ADD_GRANTS_OPTION).withDescription(ADD_GRANTS_DESC)
                .hasArg().withArgName(ADD_GRANTS_ARG_NAME).create());
        opts.addOption(new OptionBuilder().withLongOpt(DROP_USERS_OPTION).withDescription(DROP_USERS_DESC).create());
        opts.addOption(new OptionBuilder().withLongOpt(DROP_GROUPS_OPTION).withDescription(DROP_GROUPS_DESC).create());
        return opts;
    }

    @Override
    protected void parseCustomOptions(CommandLine line) {
        aclMapFile = line.getOptionValue(ACL_MAPPING_OPTION);
        domainToAppend = line.getOptionValue(APPEND_DOMAIN_OPTION);
        stripDomain = line.hasOption(STRIP_DOMAIN_OPTION);
        dropUsers = line.hasOption(DROP_USERS_OPTION);
        dropGroups = line.hasOption(DROP_GROUPS_OPTION);

        try {
            if (line.hasOption(ADD_GRANTS_OPTION)) {
                Pattern pattern = Pattern.compile(GRANT_PATTERN);
                for (String grant : line.getOptionValue(ADD_GRANTS_OPTION).split(",")) {
                    Matcher m = pattern.matcher(grant);
                    if (!m.matches())
                        throw new ConfigurationException("could not parse " + ADD_GRANTS_OPTION + " option");
                    if (m.group(1).trim().toLowerCase().equals(GROUP))
                        groupGrantsToAdd.add(m.group(2).trim(), m.group(3).trim());
                    else if (m.group(1).trim().toLowerCase().equals(USER))
                        userGrantsToAdd.add(m.group(2).trim(), m.group(3).trim());
                    else
                        throw new ConfigurationException("can only add user or group grants");
                }
            }
        } catch (Exception e) {
            throw new ConfigurationException("could not parse grants", e);
        }
    }

    @Override
    public void configure(SyncSource source, Iterator<SyncFilter> filters, SyncTarget target) {
        if (!includeAcl)
            throw new ConfigurationException("you must include ACLs in the sync to use this plugin (--" + CommonOptions.INCLUDE_ACL_OPTION + ")");

        // parse map file
        if (aclMapFile != null) {
            try {
                Pattern pattern = Pattern.compile(MAP_PATTERN);
                FileLineIterator i = new FileLineIterator(aclMapFile);
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
                            groupPermissionMap = new LinkedHashMap<String, String>();
                            permissionGroups.put(groupKey, groupPermissionMap);
                        }
                        groupPermissionMap.put(m.group(2).trim(), m.group(3).trim());
                    } else throw new RuntimeException("can only map user, group or permission");
                }
            } catch (Exception e) {
                throw new ConfigurationException("could not parse mapping file", e);
            }
        }

        if (stripDomain && domainToAppend != null)
            throw new ConfigurationException(String.format("conflicting arguments: %s + %s",
                    APPEND_DOMAIN_OPTION, STRIP_DOMAIN_OPTION));

        if (domainToAppend != null && !domainToAppend.startsWith("@")) domainToAppend = "@" + domainToAppend;
    }

    @Override
    public void filter(SyncObject obj) {
        SyncMetadata meta = obj.getMetadata();
        if (meta != null) {
            SyncAcl acl = meta.getAcl();
            if (acl == null) {
                acl = new SyncAcl();
                meta.setAcl(acl);
            }

            // map owner
            if (dropUsers) acl.setOwner(null);
            else acl.setOwner(getTargetUser(acl.getOwner()));

            // pare-down filters
            Set<String> userPermGroupMatches = new HashSet<String>(), groupPermGroupMatches = new HashSet<String>();

            // map users
            Map<String, SortedSet<String>> userGrants = new TreeMap<String, SortedSet<String>>();
            userGrants.putAll(acl.getUserGrants());
            acl.getUserGrants().clear();
            if (!dropUsers) {
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
            Map<String, SortedSet<String>> groupGrants = new TreeMap<String, SortedSet<String>>();
            groupGrants.putAll(acl.getGroupGrants());
            acl.getGroupGrants().clear();
            if (!dropGroups) {
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
        }

        getNext().filter(obj);
    }

    // TODO: if verification ever includes ACLs, reverse the ACL map here
    @Override
    public SyncObject reverseFilter(SyncObject obj) {
        return getNext().reverseFilter(obj);
    }

    @Override
    public String getName() {
        return "ACL Mapper";
    }

    @Override
    public String getDocumentation() {
        return "The ACL Mapper will map ACLs from the source system to the target using a provided mapping file. " +
                "The mapping file should be ordered by priority and will short-circuit (the first mapping found for " +
                "the source key will be chosen for the target). Note " +
                "that if a mapping is not specified for a user/group/permission, that value will remain unchanged in " +
                "the ACL of the object. You can optionally remove grants by leaving the target value empty and you can " +
                "add grants to all objects using the --" + ADD_GRANTS_OPTION + " option.\n" +
                "If you wish to migrate ACLs with your data, you will always need this plugin " +
                "unless the users, groups and permissions in both systems match exactly. Note: If you simply want to take the " +
                "default ACL of the target system, there is no need for this filter; just don't include ACLs " +
                "(omit --" + CommonOptions.INCLUDE_ACL_OPTION + ").";
    }

    protected String getTargetUser(String sourceUser) {
        String targetUser = userMap.get(sourceUser);

        // if not mapped, keep it the same
        if (targetUser == null) targetUser = sourceUser;

        // if user is mapped to "" (empty), that signifies we should remove them
        if (targetUser == null || targetUser.isEmpty()) return null;

        // append or strip domain if required
        targetUser += (domainToAppend == null) ? "" : domainToAppend;

        if (stripDomain) targetUser = targetUser.replaceAll("@.*$", "");

        return targetUser;
    }

    protected String[] getTargetPermissions(String sourceName, String sourcePermission, Set<String> groupMatches) {
        List<String> targetPermissions = new ArrayList<String>();
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

    public String getAclMapFile() {
        return aclMapFile;
    }

    public void setAclMapFile(String aclMapFile) {
        this.aclMapFile = aclMapFile;
    }

    public String getDomainToAppend() {
        return domainToAppend;
    }

    public void setDomainToAppend(String domainToAppend) {
        this.domainToAppend = domainToAppend;
    }

    public boolean isStripDomain() {
        return stripDomain;
    }

    public void setStripDomain(boolean stripDomain) {
        this.stripDomain = stripDomain;
    }

    public MultiValueMap<String, String> getGroupGrantsToAdd() {
        return groupGrantsToAdd;
    }

    public void setGroupGrantsToAdd(MultiValueMap<String, String> groupGrantsToAdd) {
        this.groupGrantsToAdd = groupGrantsToAdd;
    }

    public MultiValueMap<String, String> getUserGrantsToAdd() {
        return userGrantsToAdd;
    }

    public void setUserGrantsToAdd(MultiValueMap<String, String> userGrantsToAdd) {
        this.userGrantsToAdd = userGrantsToAdd;
    }
}
