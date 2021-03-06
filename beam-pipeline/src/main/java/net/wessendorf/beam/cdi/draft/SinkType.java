package net.wessendorf.beam.cdi.draft;

import com.fasterxml.jackson.annotation.JsonValue;

public enum SinkType {

    AMQP("ampq"),
    JMS("jms"),
    KAFKA("kafka");

    private final String typeName;

    SinkType(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Returns the actual type name of the variant type
     *
     * @return name of the type
     */
    @JsonValue
    public String getTypeName() {
        return typeName;
    }

}
