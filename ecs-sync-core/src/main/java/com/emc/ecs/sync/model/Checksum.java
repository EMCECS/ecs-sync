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

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;

public class Checksum {
    public static Checksum fromBinary(String algorithm, byte[] value) {
        return new Checksum(algorithm, value);
    }

    public static Checksum fromBase64(String algorithm, String base64Value) {
        byte[] value = DatatypeConverter.parseBase64Binary(base64Value);
        return new Checksum(algorithm, value);
    }

    public static Checksum fromHex(String algorithm, String hexValue) {
        byte[] value = DatatypeConverter.parseHexBinary(hexValue);
        return new Checksum(algorithm, value);
    }

    private final String algorithm;
    private final byte[] value;

    private Checksum(String algorithm, byte[] value) {
        this.algorithm = algorithm;
        this.value = value;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public byte[] getValue() {
        return value;
    }

    public String getBase64Value() {
        return DatatypeConverter.printBase64Binary(value);
    }

    public String getHexValue() {
        return DatatypeConverter.printHexBinary(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Checksum)) return false;

        Checksum checksum = (Checksum) o;

        if (!algorithm.equals(checksum.algorithm)) return false;
        if (!Arrays.equals(value, checksum.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = algorithm.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
