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
package com.emc.ecs.sync.config.storage;

import com.emc.ecs.sync.config.AbstractConfig;
import com.emc.ecs.sync.config.annotation.Documentation;
import com.emc.ecs.sync.config.annotation.Label;
import com.emc.ecs.sync.config.annotation.Option;
import com.emc.ecs.sync.config.annotation.StorageConfig;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Arrays;

@XmlRootElement
@StorageConfig(uriPrefix = "test:")
@Label("Simulated Storage for Testing")
@Documentation("This plugin will generate random data when used as a source, or " +
        "act as /dev/null when used as a target")
public class TestConfig extends AbstractConfig {
    public static final int DEFAULT_OBJECT_COUNT = 100;
    public static final int DEFAULT_MAX_SIZE = 1024 * 1024; // 1M
    public static final int DEFAULT_MAX_DEPTH = 5;
    public static final int DEFAULT_MAX_CHILD_COUNT = 8;
    public static final int DEFAULT_CHANCE_OF_CHILDREN = 30;
    public static final int DEFAULT_MAX_METADATA = 5;

    private long objectCount = DEFAULT_OBJECT_COUNT;
    private long maxSize = DEFAULT_MAX_SIZE;
    private int maxDepth = DEFAULT_MAX_DEPTH;
    private int maxChildCount = DEFAULT_MAX_CHILD_COUNT;
    private int chanceOfChildren = DEFAULT_CHANCE_OF_CHILDREN;
    private int maxMetadata = DEFAULT_MAX_METADATA;
    private String objectOwner;
    private String[] validUsers;
    private String[] validGroups;
    private String[] validPermissions;
    private boolean readData = true;
    private boolean discardData = true;

    @Option(orderIndex = 10, advanced = true, description = "When used as a source, the exact number of root objects to generate. Default is " + DEFAULT_OBJECT_COUNT)
    public long getObjectCount() {
        return objectCount;
    }

    public void setObjectCount(long objectCount) {
        this.objectCount = objectCount;
    }

    @Option(orderIndex = 20, advanced = true, description = "When used as a source, the maximum size of objects (actual size is random). Default is " + DEFAULT_MAX_SIZE)
    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    @Option(orderIndex = 30, advanced = true, description = "When used as a source, the maximum directory depth for children. Default is " + DEFAULT_MAX_DEPTH)
    public int getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    @Option(orderIndex = 40, advanced = true, description = "When used as a source, the maximum child count for a directory (actual child count is random). Default is " + DEFAULT_MAX_CHILD_COUNT)
    public int getMaxChildCount() {
        return maxChildCount;
    }

    public void setMaxChildCount(int maxChildCount) {
        this.maxChildCount = maxChildCount;
    }

    @Option(orderIndex = 50, advanced = true, description = "When used as a source, the percent chance that an object is a directory vs a data object. Default is " + DEFAULT_CHANCE_OF_CHILDREN)
    public int getChanceOfChildren() {
        return chanceOfChildren;
    }

    public void setChanceOfChildren(int chanceOfChildren) {
        this.chanceOfChildren = chanceOfChildren;
    }

    @Option(orderIndex = 60, advanced = true, description = "When used as a source, the maximum number of metadata tags to generate (actual number is random). Default is " + DEFAULT_MAX_METADATA)
    public int getMaxMetadata() {
        return maxMetadata;
    }

    public void setMaxMetadata(int maxMetadata) {
        this.maxMetadata = maxMetadata;
    }

    @Option(orderIndex = 70, advanced = true, description = "When used as a source, specifies the owner of every object (in the ACL)")
    public String getObjectOwner() {
        return objectOwner;
    }

    public void setObjectOwner(String objectOwner) {
        this.objectOwner = objectOwner;
    }

    @Option(orderIndex = 80, advanced = true, description = "When used as a source, specifies valid users for which to generate random grants in the ACL")
    public String[] getValidUsers() {
        return validUsers;
    }

    public void setValidUsers(String[] validUsers) {
        this.validUsers = validUsers;
    }

    @Option(orderIndex = 90, advanced = true, description = "When used as a source, specifies valid groups for which to generate random grants in the ACL")
    public String[] getValidGroups() {
        return validGroups;
    }

    public void setValidGroups(String[] validGroups) {
        this.validGroups = validGroups;
    }

    @Option(orderIndex = 100, advanced = true, description = "When used as a source, specifies valid permissions to use when generating random grants")
    public String[] getValidPermissions() {
        return validPermissions;
    }

    public void setValidPermissions(String[] validPermissions) {
        this.validPermissions = validPermissions;
    }

    @Option(orderIndex = 110, cliInverted = true, description = "When used as a target, all data is streamed from source by default. Turn this off to avoid reading data from the source")
    public boolean isReadData() {
        return readData;
    }

    public void setReadData(boolean readData) {
        this.readData = readData;
    }

    @Option(orderIndex = 120, cliInverted = true, description = "By default, all data generated or read will be discarded. Turn this off to store the object data and index in memory")
    public boolean isDiscardData() {
        return discardData;
    }

    public void setDiscardData(boolean discardData) {
        this.discardData = discardData;
    }

    public TestConfig withObjectCount(int objectCount) {
        this.objectCount = objectCount;
        return this;
    }

    public TestConfig withMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public TestConfig withMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    public TestConfig withMaxChildCount(int maxChildCount) {
        this.maxChildCount = maxChildCount;
        return this;
    }

    public TestConfig withChanceOfChildren(int chanceOfChildren) {
        this.chanceOfChildren = chanceOfChildren;
        return this;
    }

    public TestConfig withMaxMetadata(int maxMetadata) {
        this.maxMetadata = maxMetadata;
        return this;
    }

    public TestConfig withObjectOwner(String objectOwner) {
        this.objectOwner = objectOwner;
        return this;
    }

    public TestConfig withValidUsers(String[] validUsers) {
        this.validUsers = validUsers;
        return this;
    }

    public TestConfig withValidGroups(String[] validGroups) {
        this.validGroups = validGroups;
        return this;
    }

    public TestConfig withValidPermissions(String[] validPermissions) {
        this.validPermissions = validPermissions;
        return this;
    }

    public TestConfig withReadData(boolean readData) {
        this.readData = readData;
        return this;
    }

    public TestConfig withDiscardData(boolean discardData) {
        setDiscardData(discardData);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestConfig that = (TestConfig) o;

        if (objectCount != that.objectCount) return false;
        if (maxSize != that.maxSize) return false;
        if (maxDepth != that.maxDepth) return false;
        if (maxChildCount != that.maxChildCount) return false;
        if (chanceOfChildren != that.chanceOfChildren) return false;
        if (maxMetadata != that.maxMetadata) return false;
        if (readData != that.readData) return false;
        if (discardData != that.discardData) return false;
        if (objectOwner != null ? !objectOwner.equals(that.objectOwner) : that.objectOwner != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(validUsers, that.validUsers)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(validGroups, that.validGroups)) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(validPermissions, that.validPermissions);
    }

    @Override
    public int hashCode() {
        int result = (int) objectCount;
        result = 31 * result + (int) maxSize;
        result = 31 * result + maxDepth;
        result = 31 * result + maxChildCount;
        result = 31 * result + chanceOfChildren;
        result = 31 * result + maxMetadata;
        result = 31 * result + (objectOwner != null ? objectOwner.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(validUsers);
        result = 31 * result + Arrays.hashCode(validGroups);
        result = 31 * result + Arrays.hashCode(validPermissions);
        result = 31 * result + (readData ? 1 : 0);
        result = 31 * result + (discardData ? 1 : 0);
        return result;
    }
}