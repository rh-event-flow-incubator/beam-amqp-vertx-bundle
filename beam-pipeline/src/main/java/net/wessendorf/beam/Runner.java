package net.wessendorf.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.amqp.AmqpIO;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.proton.message.Message;
import org.joda.time.Duration;

import java.util.Collections;

public class Runner {

    public static void main(String[] args) throws Exception {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        // Apache Artemis:
        PCollection<JmsRecord> output = pipeline.apply(JmsIO.read()
                .withConnectionFactory(new JmsConnectionFactory("amqp://172.17.0.10:5672"))
                .withQueue("someQueue")
                .withUsername("admin")
                .withPassword("admin"));

        // Apache Qpid (using older proton-j lib)
//        PCollection<Message> output = pipeline.apply(AmqpIO.read()
//                .withMaxNumRecords(Long.MAX_VALUE)
//                .withAddresses(Collections.singletonList("amqp://172.17.0.10:5672/myQueue")));

        PCollection<String> words = output.apply(ParDo.of(new UppercaseFn()));


        words.apply(KafkaIO.<String, String>write().withBootstrapServers("172.17.0.11:9092")
                .withTopic("beam.results").withValueSerializer(StringSerializer.class)
                .withKeySerializer(StringSerializer.class).values());

        pipeline.run();

    }
}
