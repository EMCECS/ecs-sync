package sync.ui

import grails.databinding.converters.ValueConverter

class ArrayToStringByLineConverter implements ValueConverter {
    @Override
    boolean canConvert(Object value) {
        return value instanceof String[]
    }

    @Override
    Object convert(Object value) {
        return (value as String[]).join('\n')
    }

    @Override
    Class<?> getTargetType() {
        return String
    }
}
