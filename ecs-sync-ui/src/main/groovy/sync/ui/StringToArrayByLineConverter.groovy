package sync.ui

import grails.databinding.converters.ValueConverter

class StringToArrayByLineConverter implements ValueConverter {
    @Override
    boolean canConvert(Object value) {
        return value instanceof String
    }

    @Override
    Object convert(Object value) {
        return (value as String).split('\n').collect { return it.trim() }
    }

    @Override
    Class<?> getTargetType() {
        return String[].class
    }
}
