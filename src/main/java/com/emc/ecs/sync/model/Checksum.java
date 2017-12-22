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

public class Checksum {
    private String algorithm;
    private String value;

    public Checksum(String algorithm, String value) {
        this.algorithm = algorithm;
        this.value = value;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Checksum)) return false;

        Checksum checksum = (Checksum) o;

        if (!algorithm.equals(checksum.algorithm)) return false;
        if (!value.equals(checksum.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = algorithm.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
