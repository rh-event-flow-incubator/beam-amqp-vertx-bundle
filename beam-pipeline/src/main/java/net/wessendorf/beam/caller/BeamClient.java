package net.wessendorf.beam.caller;

import net.wessendorf.beam.MyMessageProcessorFn;
import net.wessendorf.beam.cdi.draft.Sink;
import net.wessendorf.beam.cdi.draft.Source;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.proton.message.Message;

import javax.enterprise.context.ApplicationScoped;


import static net.wessendorf.beam.cdi.draft.SourceType.AMQP;
import static net.wessendorf.beam.cdi.draft.SinkType.KAFKA;

@ApplicationScoped
public class BeamClient {

    @Source(type = AMQP, address = "amqp://172.17.0.10:5672", channel = "myQueue", user = "admin", password = "admin")
    @Sink(type = KAFKA, address = "172.17.0.11:9092", channel = "myTopic")
    public PCollection<String> foo(final PCollection<Message> input) {

        final PCollection<String> result = input.apply(ParDo.of(new MyMessageProcessorFn()));
        return result;
    }
}
