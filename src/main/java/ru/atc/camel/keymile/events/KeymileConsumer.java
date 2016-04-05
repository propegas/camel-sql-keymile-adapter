package ru.atc.camel.keymile.events;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.camel.model.ModelCamelContext;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.at_consulting.itsm.event.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KeymileConsumer extends ScheduledPollConsumer {

    private static KeymileEndpoint endpoint;
    private static ModelCamelContext context;
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private String[] openEventIds = {};
    private String[] openCiIds = {};

    public KeymileConsumer(KeymileEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        KeymileConsumer.endpoint = endpoint;
        //this.afterPoll();
        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
        this.setDelay(endpoint.getConfiguration().getDelay());
    }

    public static ModelCamelContext getContext() {
        // TODO Auto-generated method stub
        return context;
    }

    public static void setContext(ModelCamelContext context1) {
        context = context1;

    }

    public static void genHeartbeatMessage(Exchange exchange) {
        // TODO Auto-generated method stub
        long timestamp = System.currentTimeMillis();
        timestamp = timestamp / 1000;

        Event genevent = new Event();
        genevent.setMessage("Сигнал HEARTBEAT от адаптера");
        genevent.setEventCategory("ADAPTER");
        genevent.setObject("HEARTBEAT");
        genevent.setSeverity(PersistentEventSeverity.OK.name());
        genevent.setTimestamp(timestamp);

        genevent.setEventsource(String.format("%s", endpoint.getConfiguration().getAdaptername()));

        logger.info(" **** Create Exchange for Heartbeat Message container");
        //Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);

        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Heartbeats");
        exchange.getIn().setHeader("Type", "Heartbeats");
        exchange.getIn().setHeader("Source", String.format("%s", endpoint.getConfiguration().getAdaptername()));

    }

    public static String setRightSeverity(String severity) {
        String newseverity;

        switch (severity) {
            case "0":
                newseverity = PersistentEventSeverity.CRITICAL.name();
                break;
            case "1":
                newseverity = PersistentEventSeverity.MAJOR.name();
                break;
            case "2":
                newseverity = PersistentEventSeverity.MINOR.name();
                break;
            case "3":
                newseverity = PersistentEventSeverity.INFO.name();
                break;
            default:
                newseverity = PersistentEventSeverity.INFO.name();
                break;

        }

        return newseverity;
    }

    @Override
    protected int poll() throws Exception {

        String operationPath = endpoint.getOperationPath();

        if ("events".equals(operationPath)) return processSearchEvents();

        // only one operation implemented for now !
        throw new IllegalArgumentException("Incorrect operation: " + operationPath);
    }

    @Override
    public long beforePoll(long timeout) throws Exception {

        logger.info("*** Before Poll!!!");

        return timeout;
    }

    private void genErrorMessage(String message) {
        // TODO Auto-generated method stub
        long timestamp = System.currentTimeMillis();
        timestamp = timestamp / 1000;
        String textError = "Возникла ошибка при работе адаптера: ";
        Event genevent = new Event();
        genevent.setMessage(textError + message);
        genevent.setEventCategory("ADAPTER");
        genevent.setSeverity(PersistentEventSeverity.CRITICAL.name());
        genevent.setTimestamp(timestamp);

        genevent.setEventsource(String.format("%s", endpoint.getConfiguration().getAdaptername()));

        genevent.setStatus("OPEN");
        genevent.setHost("adapter");

        logger.info(" **** Create Exchange for Error Message container");
        Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(genevent, Event.class);

        exchange.getIn().setHeader("EventIdAndStatus", "Error_" + timestamp);
        exchange.getIn().setHeader("Timestamp", timestamp);
        exchange.getIn().setHeader("queueName", "Events");
        exchange.getIn().setHeader("Type", "Error");

        try {
            getProcessor().process(exchange);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    // "throws Exception"
    private int processSearchEvents() throws Exception {

        //Long timestamp;
        BasicDataSource dataSource = setupDataSource();

        List<HashMap<String, Object>> listOpenEvents;
        List<HashMap<String, Object>> listClosedEvents = new ArrayList<>();
        int events = 0;
        //int statuses = 0;
        try {

            // get All Closed events
            if (openEventIds.length != 0) {
                logger.info("***Try to get Closed Events***");
                listClosedEvents = getClosedEvents(dataSource);
                logger.info(String.format("***Received %d Closed Events from SQL***", listClosedEvents.size()));
            }
            // get All new (Open) events
            logger.info("***Try to get Open Events***");
            listOpenEvents = getOpenEvents(dataSource);
            logger.info(String.format("***Received %d Open Events from SQL***", listOpenEvents.size()));

            List<HashMap<String, Object>> listAllEvents = new ArrayList<>();
            listAllEvents.addAll(listOpenEvents);
            listAllEvents.addAll(listClosedEvents);

            Event genevent;

            for (int i = 0; i < listAllEvents.size(); i++) {

                genevent = genEventObj(listAllEvents.get(i));

                logger.debug("*** Create Exchange ***");

                String key = genevent.getExternalid() + "_" +
                        genevent.getStatus();

                Exchange exchange = getEndpoint().createExchange();
                exchange.getIn().setBody(genevent, Event.class);
                exchange.getIn().setHeader("EventUniqId", key);

                exchange.getIn().setHeader("Object", genevent.getObject());
                exchange.getIn().setHeader("Timestamp", genevent.getTimestamp());

                try {
                    getProcessor().process(exchange);
                    events++;

                    //File cachefile = new File("sendedEvents.dat");
                    //removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    logger.error(String.format("Error while process Exchange message: %s ", e));
                }

            }

            logger.debug(String.format("***Received %d Keymile Open Events from SQL*** ", listOpenEvents.size()));
            logger.debug(String.format("***Received %d Keymile Closed Events from SQL*** ", listClosedEvents.size()));

        } catch (Throwable e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            logger.error(String.format("Error while get Events from SQL: %s ", e));
            genErrorMessage(e.getMessage() + " " + e.toString());
            dataSource.close();
            return 0;
        } finally {
            dataSource.close();
            //return 0;
        }

        dataSource.close();

        logger.info(String.format("***Sended to Exchange messages: %d ***", events));

        return 1;
    }

    private List<HashMap<String, Object>> getClosedEvents(BasicDataSource dataSource) throws SQLException {
        // TODO Auto-generated method stub

        List<HashMap<String, Object>> list = new ArrayList<>();
        List<HashMap<String, Object>> listClosedEvents;
        Connection con = null;
        PreparedStatement pstmt;
        ResultSet resultset;

        logger.info(" **** Try to receive Closed Events ***** ");
        try {
            for (int i = 0; i < openEventIds.length; i++) {

                logger.debug(" **** Try to receive Closed Event for " + openEventIds[i]);
                con = dataSource.getConnection();
                //con.setAutoCommit(false);

                pstmt = con.prepareStatement("select tt.*, " +
                        "       ttt.unitid, " +
                        "       ttt.nodeid " +
                        "from ( " +
                        "       SELECT id, " +
                        "              alarm.neid as neid, " +
                        "              cast (coalesce(nullif(ne.name, ''), '') as text) as ne_name, " +
                        "              eventid, " +
                        "              moi, " +
                        "              perceivedseverity, " +
                        "              alarm.location as location, " +
                        "              alarmtext, " +
                        "              alarmontime, " +
                        "              cast (coalesce(nullif(alarmofftime, 0), 0) as integer) as " +
                        "                alarmofftime, " +
                        "              slotid, " +
                        "              layerid, " +
                        "              subunitid, " +
                        "              unitname, " +
                        "              unit.cfgname as cfgname, " +
                        "              unit.cfgdescription as cfgdescription, " +
                        "              cast (coalesce(nullif(unitid, 0), '0') as integer) as unitid " +
                        "       FROM public.alarm " +
                        "            left join public.ne on alarm.neid = ne.neid " +
                        "            left join public.unit on alarm.slotid = unit.slot AND alarm.neid = " +
                        "              unit.neid AND layer = ? " +
                        "       WHERE (alarm.alarmofftime is not null AND " +
                        "            alarm.alarmofftime != ?) AND  " +
                        "            alarm.id = CAST(? as integer) " +
                        "     ) tt " +
                        "     inner join  " +
                        "     ( " +
                        "       select t1.*, " +
                        "              t2.unitid " +
                        "       from ( " +
                        "              SELECT nodeid, " +
                        "                     treehierarchy, " +
                        "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 2), ''), '0') as integer) as x , " +
                        "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 3), ''), '0') as integer) as y , " +
                        "                     split_part(treehierarchy, '.', 4) as z, " +
                        "                     typeclass " +
                        "              FROM public.node t1 " +
                        "              where t1.typeclass in (1, 2) and " +
                        "                    t1.type <> '' /*and t2.z = ''*/ " +
                        "              order by t1.nodeid " +
                        "            ) t1 " +
                        "            left join public.unit t2 on t1.x = t2.neid and t1.y = t2.slot and " +
                        "              t2.layer = 0 " +
                        "     ) ttt on tt.unitid = ttt.unitid ");
                // +" LIMIT ?;");

                pstmt.setInt(1, 0); // layer
                pstmt.setInt(2, 0); // alarmofftime
                pstmt.setString(3, openEventIds[i]); // id

                logger.debug("DB query: " + pstmt.toString());
                resultset = pstmt.executeQuery();
                //con.commit();

                listClosedEvents = convertRStoList(resultset);
                if (listClosedEvents != null) {
                    list.addAll(listClosedEvents);
                }

                List<HashMap<String, Object>> listEventsById = getEventById(openEventIds[i], dataSource);
                // if no alarm id in table
                if (listEventsById.size() == 0) {

                    logger.info("DB alarm: " + openEventIds[i] + " not found");
                    int columns = 3;
                    HashMap<String, Object> row = new HashMap<>(columns);

                    logger.debug("Put \"id\": " + openEventIds[i]);
                    logger.debug("Put \"ciid\": " + openCiIds[i]);
                    row.put("id", openEventIds[i]);
                    row.put("ciid", openCiIds[i]);

                    logger.debug("Put \"closed\" mark to hash");
                    row.put("closed", "1");

                    list.add(row);
                }

                resultset.close();
                pstmt.close();

                if (con != null) con.close();
            }

            return list;

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
            logger.error(String.format("Error while SQL execution: %s ", e));

            if (con != null) con.close();

            //return null;
            throw e;

        } catch (Throwable e) { //send error message to the same queue
            // TODO Auto-generated catch block
            logger.error(String.format("Error while execution: %s ", e));
            //genErrorMessage(e.getMessage());
            // 0;
            throw e;
        } finally {
            if (con != null) con.close();

            //return list;
        }

    }

    private List<HashMap<String, Object>> getOpenEvents(BasicDataSource dataSource) throws SQLException {
        // TODO Auto-generated method stub

        List<HashMap<String, Object>> listOpenedEvents;

        Connection con = null;
        PreparedStatement pstmt;
        ResultSet resultset;

        logger.info(" **** Try to receive Open Events ***** ");
        try {
            con = dataSource.getConnection();
            //con.setAutoCommit(false);

            pstmt = con.prepareStatement("select tt.*, " +
                    "       ttt.unitid, " +
                    "       ttt.nodeid " +
                    "from ( " +
                    "       SELECT id, " +
                    "              alarm.neid as neid, " +
                    "              cast (coalesce(nullif(ne.name, ''), '') as text) as ne_name, " +
                    "              eventid, " +
                    "              moi, " +
                    "              perceivedseverity, " +
                    "              alarm.location as location, " +
                    "              alarmtext, " +
                    "              alarmontime, " +
                    "              cast (coalesce(nullif(alarmofftime, 0), 0) as integer) as " +
                    "                alarmofftime, " +
                    "              slotid, " +
                    "              layerid, " +
                    "              subunitid, " +
                    "              unitname, " +
                    "              unit.cfgname as cfgname, " +
                    "              unit.cfgdescription as cfgdescription, " +
                    "              cast (coalesce(nullif(unitid, 0), '0') as integer) as unitid " +
                    "       FROM public.alarm " +
                    "            left join public.ne on alarm.neid = ne.neid " +
                    "            left join public.unit on alarm.slotid = unit.slot AND alarm.neid = " +
                    "              unit.neid AND layer = '0' " +
                    "       WHERE (alarm.alarmofftime is null OR " +
                    "             alarm.alarmofftime = ?) " +
                    "     ) tt " +
                    "     inner join  " +
                    "     ( " +
                    "       select t1.*, " +
                    "              t2.unitid " +
                    "       from ( " +
                    "              SELECT nodeid, " +
                    "                     treehierarchy, " +
                    "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 2), ''), '0') as integer) as x , " +
                    "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 3), ''), '0') as integer) as y , " +
                    "                     split_part(treehierarchy, '.', 4) as z, " +
                    "                     typeclass " +
                    "              FROM public.node t1 " +
                    "              where t1.typeclass in (1, 2) and " +
                    "                    t1.type <> '' /*and t2.z = ''*/ " +
                    "              order by t1.nodeid " +
                    "            ) t1 " +
                    "            left join public.unit t2 on t1.x = t2.neid and t1.y = t2.slot and " +
                    "              t2.layer = ? " +
                    "     ) ttt on tt.unitid = ttt.unitid ");
            // +" LIMIT ?;");
            //pstmt.setString(1, "");
            pstmt.setInt(1, 0); //alarmofftime
            pstmt.setInt(2, 0); //layer

            logger.debug("DB query: " + pstmt.toString());
            resultset = pstmt.executeQuery();

            listOpenedEvents = convertRStoList(resultset);

            resultset.close();
            pstmt.close();

            if (con != null) con.close();

            logger.debug(" **** Saving Received opened events's IDs and CI IDs ****");

            openEventIds = new String[]{};
            openCiIds = new String[]{};
            if (listOpenedEvents != null) {
                for (int i = 0; i < listOpenedEvents.size(); i++) {
                    openEventIds = (String[]) ArrayUtils.add(openEventIds, listOpenedEvents.get(i).get("id").toString());
                    openCiIds = (String[]) ArrayUtils.add(openCiIds, listOpenedEvents.get(i).get("nodeid").toString());
                    logger.debug(" **** Saving ID: " + listOpenedEvents.get(i).get("id").toString());
                    logger.debug(" **** Saving CI ID: " + listOpenedEvents.get(i).get("nodeid").toString());
                }
            }

            logger.info(" **** Saved " + openEventIds.length + " Opened Events ****");

            return listOpenedEvents;

        } catch (SQLException e) {
            //e.printStackTrace();
            logger.error(String.format("Error while SQL execution: %s ", e));

            if (con != null) con.close();

            //return null;
            throw e;

        } catch (Throwable e) { //send error message to the same queue
            logger.error(String.format("Error while execution: %s ", e));
            //genErrorMessage(e.getMessage());
            // 0;
            throw e;
        } finally {
            if (con != null) con.close();

            //return list;
        }

    }

    private List<HashMap<String, Object>> getEventById(String id, BasicDataSource dataSource) throws SQLException {
        // TODO Auto-generated method stub

        List<HashMap<String, Object>> list;

        Connection con = null;
        PreparedStatement pstmt;
        ResultSet resultset;

        logger.debug(" **** Try to get Event by ID for " + id);
        try {
            con = dataSource.getConnection();
            //con.setAutoCommit(false);

            pstmt = con.prepareStatement("SELECT  " +
                    "id FROM public.alarm " +
                    "WHERE alarm.id = cast(? as INTEGER);");
            // +" LIMIT ?;");
            //pstmt.setString(1, "");
            pstmt.setString(1, id); // id

            logger.debug("DB query: " + pstmt.toString());
            resultset = pstmt.executeQuery();

            list = convertRStoList(resultset);

            resultset.close();
            pstmt.close();

            if (con != null) con.close();

            return list;

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
            logger.error(String.format("Error while SQL execution: %s ", e));

            if (con != null) con.close();

            //return null;
            throw e;

        } catch (Throwable e) { //send error message to the same queue
            // TODO Auto-generated catch block
            logger.error(String.format("Error while execution: %s ", e));
            //genErrorMessage(e.getMessage());
            // 0;
            throw e;
        } finally {
            if (con != null) con.close();

            //return list;
        }

    }

    private Event genEventObj(HashMap<String, Object> alarm) {
        Event event;
        event = new Event();

        logger.debug(String.format("ID: %s ", alarm.get("id").toString()));

        // if no record in DB
        if (alarm.containsKey("closed")) {

            logger.debug("Has \"closed\" mark in hash");
            long timestamp = System.currentTimeMillis();
            timestamp = timestamp / 1000;
            event.setHost("");
            event.setCi(String.format("%s:%s",
                    endpoint.getConfiguration().getSource(), alarm.get("ciid").toString())
            );
            event.setExternalid(alarm.get("id").toString());
            event.setTimestamp(timestamp);
            event.setSeverity(PersistentEventSeverity.OK.name());
            event.setStatus("CLOSED");
            event.setMessage("Ошибка на оборудовании Keymile устранена");

        } else {
            Long eventclosedate;
            Long eventopendate;
            eventopendate = (long) Integer.parseInt(alarm.get("alarmontime").toString());
            eventclosedate = (long) Integer.parseInt(alarm.get("alarmofftime").toString());

            logger.debug(String.format("alarmofftime: %s, %d ", alarm.get("alarmofftime").toString(), eventclosedate));

            event.setHost(alarm.get("ne_name").toString());
            event.setObject(alarm.get("moi").toString());
            event.setExternalid(alarm.get("id").toString());

            String deviceid = alarm.get("nodeid").toString();
            if ("0".equals(deviceid))
                deviceid = null;
            else
                deviceid = String.format("%s:%s", endpoint.getConfiguration().getSource(), deviceid);

            event.setCi(deviceid);
            event.setSeverity(setRightSeverity(alarm.get("perceivedseverity").toString()));
            event.setMessage(String.format("Ошибка на оборудовании Keymile (%s): %s (%s)",
                    alarm.get("ne_name").toString(),
                    alarm.get("alarmtext").toString(),
                    alarm.get("location").toString()
            ));
            event.setCategory("NETWORK");
            if (eventclosedate != 0) {
                event.setTimestamp(eventclosedate);
                event.setStatus("CLOSED");
            } else {
                event.setTimestamp(eventopendate);
                event.setStatus("OPEN");
            }
        }

        event.setEventsource(endpoint.getConfiguration().getSource());

        logger.debug(event.toString());

        return event;
    }

    private List<HashMap<String, Object>> convertRStoList(ResultSet resultset) {

        List<HashMap<String, Object>> list = new ArrayList<>();

        try {
            ResultSetMetaData md = resultset.getMetaData();
            int columns = md.getColumnCount();

            logger.debug("DB SQL columns count: " + columns);

            while (resultset.next()) {
                HashMap<String, Object> row = new HashMap<>(columns);
                for (int i1 = 1; i1 <= columns; ++i1) {
                    logger.debug("DB SQL getColumnLabel: " + md.getColumnLabel(i1));
                    logger.debug("DB SQL getObject: " + resultset.getObject(i1));
                    row.put(md.getColumnLabel(i1), resultset.getObject(i1));
                }
                list.add(row);
            }

            return list;

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

            return null;

        }
    }

    private BasicDataSource setupDataSource() {

        String url = String.format("jdbc:postgresql://%s:%s/%s",
                endpoint.getConfiguration().getPostgresqlHost(), endpoint.getConfiguration().getPostgresqlPort(),
                endpoint.getConfiguration().getPostgresqlDb());

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername(endpoint.getConfiguration().getUsername());
        ds.setPassword(endpoint.getConfiguration().getPassword());
        ds.setUrl(url);

        return ds;
    }

    public enum PersistentEventSeverity {
        OK, INFO, WARNING, MINOR, MAJOR, CRITICAL;

        public static PersistentEventSeverity fromValue(String v) {
            return valueOf(v);
        }

        public String value() {
            return name();
        }
    }

}