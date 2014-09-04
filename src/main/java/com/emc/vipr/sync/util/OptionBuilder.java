package com.emc.vipr.sync.util;

import org.apache.commons.cli.Option;

public class OptionBuilder {
    private String description;
    private String longOpt;
    private int argNum = Option.UNINITIALIZED;
    private char valueSep;

    private String argName;

    public OptionBuilder withDescription(String description) {
        this.description = description;
        return this;
    }

    public OptionBuilder withLongOpt(String longOpt) {
        this.longOpt = longOpt;
        return this;
    }

    public OptionBuilder hasArg() {
        argNum = 1;
        return this;
    }

    public OptionBuilder hasArgs() {
        argNum = Option.UNLIMITED_VALUES;
        return this;
    }

    public OptionBuilder withArgName(String argName) {
        this.argName = argName;
        return this;
    }

    public OptionBuilder withValueSeparator(char valueSep) {
        this.valueSep = valueSep;
        return this;
    }

    public Option create() {
        // create the option
        Option option = new Option(null, description);

        // set the option properties
        option.setLongOpt(longOpt);
        option.setArgs(argNum);
        option.setValueSeparator(valueSep);
        option.setArgName(argName);
        return option;
    }
}
