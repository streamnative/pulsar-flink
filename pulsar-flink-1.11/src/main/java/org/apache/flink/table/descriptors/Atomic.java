package org.apache.flink.table.descriptors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT_DERIVE_SCHEMA;
import static org.apache.flink.table.descriptors.AtomicValidator.FORMAT_TYPE_VALUE;

public class Atomic extends FormatDescriptor{

    //private boolean useExtendFields;
    private String className;
    /**
     * Format descriptor for JSON.
     */
    public Atomic() {
        super(FORMAT_TYPE_VALUE, 1);
    }

    /*public Atomic useExtendFields(boolean useExtendFields){
        this.useExtendFields = useExtendFields;
        return this;
    }*/

    public Atomic setClass(String className){
        this.className = className;
        return this;
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        final DescriptorProperties properties = new DescriptorProperties();
        properties.putString(AtomicValidator.FORMAT_CLASS_NAME, className);
        //properties.putBoolean(ConnectorDescriptorValidator.CONNECTOR + "." + PulsarOptions.USE_EXTEND_FIELD, useExtendFields);
        return properties.asMap();
    }
}
