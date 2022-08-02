/*
 * Copyright (c) 2021 Dell Inc. or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.ecs.sync.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ChecksumTest {
    @Test
    public void testConversions() {
        String alg = "MD5";
        String hexValue = "D731E47A8731774B9B88BA89DBB1C7EC";

        // base value from hex
        Checksum checksum = Checksum.fromHex(alg, hexValue);

        // convert out to b64 and back
        checksum = Checksum.fromBase64(alg, checksum.getBase64Value());

        // convert out to hex and back
        checksum = Checksum.fromHex(alg, checksum.getHexValue());

        // just for kicks, dump binary and back (should be no conversion done here)
        checksum = Checksum.fromBinary(alg, checksum.getValue());

        Assertions.assertEquals(alg, checksum.getAlgorithm());
        Assertions.assertEquals(hexValue, checksum.getHexValue());
    }
}
