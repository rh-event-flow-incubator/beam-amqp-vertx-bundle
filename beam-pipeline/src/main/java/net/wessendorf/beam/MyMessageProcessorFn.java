package net.wessendorf.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

public class MyMessageProcessorFn extends DoFn<Message, String> {

    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        Section body = c.element().getBody();

        if (body instanceof AmqpValue) {
            c.output(((AmqpValue) body).getValue().toString().toUpperCase());
        }
    }
}
