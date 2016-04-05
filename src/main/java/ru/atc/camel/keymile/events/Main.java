package ru.atc.camel.keymile.events;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.model.ModelCamelContext;
import org.apache.camel.model.dataformat.JsonDataFormat;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.processor.idempotent.FileIdempotentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.at_consulting.itsm.event.Event;

import javax.jms.ConnectionFactory;
import java.io.File;
import java.util.Objects;

public final class Main {

    private static ModelCamelContext context;
    private static String activemqPort;
    private static String activemqIp;
    //public static String postgresql_ip;
    //public static String postgresql_port;
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final int CACHE_SIZE = 2500;
    private static final int MAX_FILE_SIZE = 512000;

    private Main() {

    }

    public static void main(String[] args) throws Exception {

        logger.info("Starting Custom Apache Camel component example");
        logger.info("Press CTRL+C to terminate the JVM");

        if (args.length == 2) {
            activemqPort = args[1];
            activemqIp = args[0];
        }

        if (activemqPort == null || "".equals(activemqPort))
            activemqPort = "61616";
        if (activemqIp == null || Objects.equals(activemqIp, ""))
            activemqIp = "172.20.19.195";

        logger.info("activemqIp: " + activemqIp);
        logger.info("activemqPort: " + activemqPort);

        org.apache.camel.main.Main main = new org.apache.camel.main.Main();
        main.enableHangupSupport();

        main.addRouteBuilder(new RouteBuilder() {

            @Override
            public void configure() throws Exception {

                JsonDataFormat myJson = new JsonDataFormat();
                myJson.setPrettyPrint(true);
                myJson.setLibrary(JsonLibrary.Jackson);
                myJson.setJsonView(Event.class);

                context = getContext();

                PropertiesComponent properties = new PropertiesComponent();
                properties.setLocation("classpath:keymile.properties");
                context.addComponent("properties", properties);

                ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                        "tcp://" + activemqIp + ":" + activemqPort);
                context.addComponent("activemq", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));

                logger.info("*****context: " + context);
                logger.info("*****username: " + "{{username}}");

                KeymileConsumer.setContext(context);

                File cachefile = new File("sendedEvents.dat");
                //cachefile.createNewFile();

                from(new StringBuilder()
                        .append("keymile://events?")
                        .append("delay={{delay}}&")
                        .append("username={{username}}&")
                        .append("password={{password}}&")
                        .append("postgresqlHost={{postgresql_host}}&")
                        .append("postgresqlDb={{postgresql_db}}&")
                        .append("postgresqlPort={{postgresql_port}}&")
                        .append("source={{source}}&")
                        .append("adaptername={{adaptername}}")
                        .toString())
                        .choice()
                        .when(header("Type").isEqualTo("Error"))
                        .marshal(myJson)
                        .to("activemq:{{eventsqueue}}")
                        .log("Error: ${id} ${header.EventUniqId}")

                        .otherwise()
                        .idempotentConsumer(
                                header("EventUniqId"),
                                FileIdempotentRepository.fileIdempotentRepository(
                                        cachefile, CACHE_SIZE, MAX_FILE_SIZE
                                )
                        )

                        .marshal(myJson)

                        .to("activemq:{{eventsqueue}}")
                        .log("*** NEW EVENT: ${id} ${header.EventIdAndStatus}");

                // Heartbeats
                from("timer://foo?period={{heartbeatsdelay}}")
                        .process(new Processor() {
                            public void process(Exchange exchange) throws Exception {
                                KeymileConsumer.genHeartbeatMessage(exchange);
                            }
                        })

                        .marshal(myJson)
                        .to("activemq:{{heartbeatsqueue}}")
                        .log("*** Heartbeat: ${id}");
            }
        });

        main.run();

    }

}