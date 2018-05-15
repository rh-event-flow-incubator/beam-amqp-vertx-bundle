package net.wessendorf.beam;

import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

public class UppercaseFn extends DoFn<JmsRecord, String> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        String body = c.element().getPayload();

        System.out.println("DA ......" + body);

        c.output(body.toUpperCase());

    }
}
//public class UppercaseFn extends DoFn<Message, String> {
//    @DoFn.ProcessElement
//    public void processElement(ProcessContext c) {
//        Section body = c.element().getBody();
//
//        System.out.println("DA ......" + body);
//
//        if (body instanceof AmqpValue) {
//            c.output(((AmqpValue) body).getValue().toString().toUpperCase());
//        }
//    }
//}
