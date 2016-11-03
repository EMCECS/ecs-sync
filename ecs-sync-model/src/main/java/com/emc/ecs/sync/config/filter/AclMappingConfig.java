/*
 * Copyright 2013-2016 EMC Corporation. All Rights Reserved.
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
package com.emc.ecs.sync.config.filter;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.FilterConfig;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@FilterConfig(cliName = "acl-mapping")
@Label("ACL Mapper")
@Documentation("The ACL Mapper will map ACLs from the source system to the target using a provided mapping file. " +
        "The mapping file should be ordered by priority and will short-circuit (the first mapping found for " +
        "the source key will be chosen for the target). Note " +
        "that if a mapping is not specified for a user/group/permission, that value will remain unchanged in " +
        "the ACL of the object. You can optionally remove grants by leaving the target value empty and you can " +
        "add grants to all objects using the --acl-add-grants option.\n" +
        "If you wish to migrate ACLs with your data, you will always need this plugin " +
        "unless the users, groups and permissions in both systems match exactly. Note: If you simply want to take the " +
        "default ACL of the target system, there is no need for this filter; just don't sync ACLs " +
        "(this is the default behavior)")
public class AclMappingConfig extends AbstractConfig {
    private String aclMapFile;
    private String aclMapInstructions;
    private String aclAppendDomain;
    private boolean aclStripDomain;
    private String aclAddGrants;
    private boolean aclStripUsers;
    private boolean aclStripGroups;

    @Option(locations = Option.Location.CLI,
            description = "Path to a file that contains the mapping of identities and permissions from source to target. " +
                    "Each entry is on a separate  line and specifies a group/user/permission source and target name[s] like so:\n" +
                    "group.<source_group>=<target_group>\n" +
                    "user.<source_user>=<target_user>\n" +
                    "permission.<source_perm>=<target_perm>[,<target_perm>..]\n" +
                    "You can also pare down permissions that are redundant in the target system by using permission groups. I.e.:\n" +
                    "permission1.WRITE=READ_WRITE\n" +
                    "permission1.READ=READ\n" +
                    "will pare down separate READ and WRITE permissions into one READ_WRITE/READ (note the ordering by " +
                    "priority). Groups are processed before straight mappings. Leave the target value blank to flag an " +
                    "identity/permission that should be removed (perhaps it does not exist in the target system)")
    public String getAclMapFile() {
        return aclMapFile;
    }

    public void setAclMapFile(String aclMapFile) {
        this.aclMapFile = aclMapFile;
    }

    @Option(locations = Option.Location.Form,
            description = "The mapping of identities and permissions from source to target. Each entry is on a separate " +
                    "line and specifies a group/user/permission source and target name[s] like so:\n" +
                    "group.<source_group>=<target_group>\n" +
                    "user.<source_user>=<target_user>\n" +
                    "permission.<source_perm>=<target_perm>[,<target_perm>..]\n" +
                    "You can also pare down permissions that are redundant in the target system by using permission groups. I.e.:\n" +
                    "permission1.WRITE=READ_WRITE\n" +
                    "permission1.READ=READ\n" +
                    "will pare down separate READ and WRITE permissions into one READ_WRITE/READ (note the ordering by " +
                    "priority). Groups are processed before straight mappings. Leave the target value blank to flag an " +
                    "identity/permission that should be removed (perhaps it does not exist in the target system)")
    public String getAclMapInstructions() {
        return aclMapInstructions;
    }

    public void setAclMapInstructions(String aclMapInstructions) {
        this.aclMapInstructions = aclMapInstructions;
    }

    @Option(description = "Appends a directory realm/domain to each user that is mapped. Useful when mapping POSIX users to LDAP identities")
    public String getAclAppendDomain() {
        return aclAppendDomain;
    }

    public void setAclAppendDomain(String aclAppendDomain) {
        this.aclAppendDomain = aclAppendDomain;
    }

    @Option(description = "Strips the directory realm/domain from each user that is mapped. Useful when mapping LDAP identities to POSIX users")
    public boolean isAclStripDomain() {
        return aclStripDomain;
    }

    public void setAclStripDomain(boolean aclStripDomain) {
        this.aclStripDomain = aclStripDomain;
    }

    @Option(description = "Adds a comma-separated list of grants to all objects synced to the target system. Syntax is like so (repeats are allowed):\n" +
            "group.<target_group>=<target_perm>,user.<target_user>=<target_perm>")
    public String getAclAddGrants() {
        return aclAddGrants;
    }

    public void setAclAddGrants(String aclAddGrants) {
        this.aclAddGrants = aclAddGrants;
    }

    @Option(description = "Drops all users from each object's ACL. Use with --acl-add-grants to add specific user grants instead")
    public boolean isAclStripUsers() {
        return aclStripUsers;
    }

    public void setAclStripUsers(boolean aclStripUsers) {
        this.aclStripUsers = aclStripUsers;
    }

    @Option(description = "Drops all groups from each object's ACL. Use with --acl-add-grants to add specific group grants instead")
    public boolean isAclStripGroups() {
        return aclStripGroups;
    }

    public void setAclStripGroups(boolean aclStripGroups) {
        this.aclStripGroups = aclStripGroups;
    }
}
