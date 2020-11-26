package org.apache.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;

public class AtomicValidator extends FormatDescriptorValidator {
    public static final String FORMAT_TYPE_VALUE = "atomic";
    public static final String FORMAT_ATOMIC_SCHEMA = "format.atomic-schema";
    public static final String FORMAT_CLASS_NAME = "format.classname";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        //TODO add validate for classname
    }
}
