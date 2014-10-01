package com.emc.vipr.sync.model;

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
