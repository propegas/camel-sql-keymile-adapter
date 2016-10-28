package ru.atc.camel.keymile.events;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
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
import ru.atc.adapters.type.Event;

import javax.jms.ConnectionFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static ru.atc.adapters.message.CamelMessageManager.genHeartbeatMessage;

public final class Main {

    private static final Logger logger = LoggerFactory.getLogger("mainLogger");
    private static final Logger loggerErrors = LoggerFactory.getLogger("errorsLogger");
    private static final int CACHE_SIZE = 2500;
    private static final int MAX_FILE_SIZE = 512000;
    private static ModelCamelContext context;
    private static String activemqPort;
    private static String activemqIp;
    private static String adaptername;

    private Main() {

    }

    public static void main(String[] args) throws Exception {

        logger.info("Starting Custom Apache Camel component example");
        logger.info("Press CTRL+C to terminate the JVM");

        try {
            // get Properties from file
            InputStream input = new FileInputStream("keymile.properties");
            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            adaptername = prop.getProperty("adaptername");
            activemqIp = prop.getProperty("activemq.ip");
            activemqPort = prop.getProperty("activemq.port");
        } catch (IOException ex) {
            logger.error("Error while open and parsing properties file", ex);
        }

        logger.info("**** adaptername: " + adaptername);
        logger.info("activemqIp: " + activemqIp);
        logger.info("activemqPort: " + activemqPort);

        org.apache.camel.main.Main main = new org.apache.camel.main.Main();

        main.addRouteBuilder(new IntegrationRoute());

        main.run();

    }

    private static class IntegrationRoute extends RouteBuilder {
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

            File cachefile = new File("sendedEvents.dat");

            from(new StringBuilder()
                    .append("keymile://events?")
                    .append("delay={{delay}}&")
                    .append("username={{username}}&")
                    .append("password={{password}}&")
                    .append("postgresqlHost={{postgresqlHost}}&")
                    .append("postgresqlDb={{postgresqlDb}}&")
                    .append("postgresqlPort={{postgresqlPort}}&")
                    .append("source={{source}}&")
                    .append("adaptername={{adaptername}}")
                    .toString())

                    .marshal(myJson)
                    .choice()

                    .when(header("Type").isEqualTo("Error"))
                    .to("activemq:{{errorsqueue}}")
                    .log("*** Error: ${id} ")
                    .log(LoggingLevel.ERROR, logger, "*** NEW ERROR BODY: ${in.body}")

                    .otherwise()
                    .idempotentConsumer(
                            header("EventUniqId"),
                            FileIdempotentRepository.fileIdempotentRepository(
                                    cachefile, CACHE_SIZE, MAX_FILE_SIZE
                            )
                    )

                    .to("activemq:{{eventsqueue}}")
                    .log("*** NEW EVENT: ${id} ${header.EventIdAndStatus}")
                    .log(LoggingLevel.DEBUG, logger, "*** NEW EVENT BODY: ${in.body}");

            // Heartbeats
            from("timer://foo?period={{heartbeatsdelay}}")
                    .process(new Processor() {
                        @Override
                        public void process(Exchange exchange) throws Exception {
                            genHeartbeatMessage(exchange, adaptername);
                        }
                    })
                    .marshal(myJson)
                    .to("activemq:{{heartbeatsqueue}}")
                    .log("*** Heartbeat: ${id}")
                    .log(LoggingLevel.DEBUG, logger, "*** NEW HEARTBEAT BODY: ${in.body}");

        }
    }
}