package net.wessendorf.amqp;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;

import java.util.logging.Logger;

import static org.apache.qpid.proton.Proton.message;

public class MyQpidApp extends AbstractVerticle {

    private static final Logger LOGGER = Logger.getLogger(MyQpidApp.class.getName());

    @Override
    public void start(Future<Void> future) {


        final ProtonClient client = ProtonClient.create(vertx);

        //client.connect("172.17.0.10", 5672, connectResult -> {
        // Connect, then use the event loop thread to process things thereafter
         client.connect("172.17.0.3", 5672, "admin","admin", connectResult -> {
            if (connectResult.succeeded()) {

                LOGGER.info("connectd");

                connectResult.result().setContainer("my-container/client-id").openHandler(openResult -> {
                    if (openResult.succeeded()) {
                        ProtonConnection connection = openResult.result();
                        // Create senders, receivers etc..
                        //send(connection);

                        connection.createSender("myQueue").openHandler(openSenderResult -> {
                            if (openSenderResult.succeeded()) {
                                ProtonSender sender = openSenderResult.result();

                                vertx.setPeriodic(250, id -> {

                                    Message message = message();
                                    message.setBody(new AmqpValue("Hello World"));

                                    // Send message, providing an onUpdated delivery handler that prints updates
                                    sender.send(message, delivery -> {
                                        System.out.println(String.format("Message received by server: remote state=%s, remotely settled=%s",
                                                delivery.getRemoteState(), delivery.remotelySettled()));
                                    });

                                });
                            }
                        }).open();


                        // receive
                        connection.createReceiver("myQueue").handler((delivery, msg) -> {
                            Section body = msg.getBody();
                            if (body instanceof AmqpValue) {
                                System.out.println("Received message with content: " + ((AmqpValue) body).getValue() + " from: " + msg.getCreationTime());
                            }
                            // By default, the receiver automatically accepts (and settles) the delivery
                            // when the handler returns if no other disposition has already been applied.
                        }).open();


                    }
                }).open();
            }
        });
    }

}
