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
