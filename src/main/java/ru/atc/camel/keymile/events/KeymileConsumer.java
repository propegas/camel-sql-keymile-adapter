package ru.atc.camel.keymile.events;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.atc.adapters.type.Event;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static ru.atc.adapters.message.CamelMessageManager.genAndSendErrorMessage;

public class KeymileConsumer extends ScheduledPollConsumer {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static KeymileEndpoint endpoint;
    private String[] openEventIds = {};
    private String[] openCiIds = {};

    public KeymileConsumer(KeymileEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        KeymileConsumer.endpoint = endpoint;
        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
        this.setDelay(endpoint.getConfiguration().getDelay());
    }

    public static String setRightSeverity(String severity) {
        String newseverity;

        switch (severity) {
            case "0":
                newseverity = Event.PersistentEventSeverity.CRITICAL.name();
                break;
            case "1":
                newseverity = Event.PersistentEventSeverity.MAJOR.name();
                break;
            case "2":
                newseverity = Event.PersistentEventSeverity.MINOR.name();
                break;
            case "3":
                newseverity = Event.PersistentEventSeverity.INFO.name();
                break;
            default:
                newseverity = Event.PersistentEventSeverity.INFO.name();
                break;

        }

        return newseverity;
    }

    @Override
    protected int poll() throws Exception {

        String operationPath = endpoint.getOperationPath();

        if ("events".equals(operationPath))
            return processSearchEvents();

        // only one operation implemented for now !
        throw new IllegalArgumentException("Incorrect operation: " + operationPath);
    }

    @Override
    public long beforePoll(long timeout) throws Exception {

        logger.info("*** Before Poll!!!");

        return timeout;
    }

    private int processSearchEvents() {

        BasicDataSource dataSource = setupDataSource();

        List<HashMap<String, Object>> listOpenEvents;
        List<HashMap<String, Object>> listClosedEvents = new ArrayList<>();
        int events = 0;

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

            events = genAndProcessEvents(listAllEvents);

            logger.debug(String.format("***Received %d Keymile Open Events from SQL*** ", listOpenEvents.size()));
            logger.debug(String.format("***Received %d Keymile Closed Events from SQL*** ", listClosedEvents.size()));

        } catch (Exception e) {
            genErrorMessage("Error while get Events from DB", e);
            return 0;
        } finally {
            closeConnectionsAndDS(dataSource);
        }

        logger.info(String.format("***Sended to Exchange messages: %d ***", events));

        return 1;
    }

    private int genAndProcessEvents(List<HashMap<String, Object>> listAllEvents) {
        Event genevent;

        int events = 0;
        for (HashMap<String, Object> listAllEvent : listAllEvents) {

            genevent = genEventObj(listAllEvent);

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
            } catch (Exception e) {
                genErrorMessage("Error while process Exchange message ", e);
            }

        }
        return events;
    }

    private void genErrorMessage(String message, Exception exception) {
        genAndSendErrorMessage(this, message, exception,
                endpoint.getConfiguration().getAdaptername());
    }

    private List<HashMap<String, Object>> getClosedEvents(BasicDataSource dataSource) throws SQLException {

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

                pstmt = con.prepareStatement("SELECT tt.*, " +
                        "       ttt.unitid, " +
                        "       ttt.nodeid " +
                        "FROM ( " +
                        "       SELECT id, " +
                        "              alarm.neid AS neid, " +
                        "              cast (coalesce(nullif(ne.name, ''), '') AS TEXT) AS ne_name, " +
                        "              eventid, " +
                        "              moi, " +
                        "              perceivedseverity, " +
                        "              alarm.location AS location, " +
                        "              alarmtext, " +
                        "              alarmontime, " +
                        "              cast (coalesce(nullif(alarmofftime, 0), 0) AS INTEGER) AS " +
                        "                alarmofftime, " +
                        "              slotid, " +
                        "              layerid, " +
                        "              subunitid, " +
                        "              unitname, " +
                        "              unit.cfgname AS cfgname, " +
                        "              unit.cfgdescription AS cfgdescription, " +
                        "              cast (coalesce(nullif(unitid, 0), '0') AS INTEGER) AS unitid " +
                        "       FROM public.alarm " +
                        "            LEFT JOIN public.ne ON alarm.neid = ne.neid " +
                        "            LEFT JOIN public.unit ON alarm.slotid = unit.slot AND alarm.neid = " +
                        "              unit.neid AND layer = ? " +
                        "       WHERE (alarm.alarmofftime IS NOT NULL AND " +
                        "            alarm.alarmofftime != ?) AND  " +
                        "            alarm.id = CAST(? AS INTEGER) " +
                        "     ) tt " +
                        "     INNER JOIN  " +
                        "     ( " +
                        "       SELECT t1.*, " +
                        "              t2.unitid " +
                        "       FROM ( " +
                        "              SELECT nodeid, " +
                        "                     treehierarchy, " +
                        "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 2), ''), '0') AS INTEGER) AS x , " +
                        "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 3), ''), '0') AS INTEGER) AS y , " +
                        "                     split_part(treehierarchy, '.', 4) AS z, " +
                        "                     typeclass " +
                        "              FROM public.node t1 " +
                        "              WHERE t1.typeclass IN (1, 2) AND " +
                        "                    t1.type <> '' /*and t2.z = ''*/ " +
                        "              ORDER BY t1.nodeid " +
                        "            ) t1 " +
                        "            LEFT JOIN public.unit t2 ON t1.x = t2.neid AND t1.y = t2.slot AND " +
                        "              t2.layer = 0 " +
                        "     ) ttt ON tt.unitid = ttt.unitid ");

                pstmt.setInt(1, 0); // layer
                pstmt.setInt(2, 0); // alarmofftime
                pstmt.setString(3, openEventIds[i]); // id

                logger.debug("DB query: " + pstmt.toString());
                resultset = pstmt.executeQuery();

                listClosedEvents = convertRStoList(resultset);
                if (listClosedEvents != null) {
                    list.addAll(listClosedEvents);
                }

                List<HashMap<String, Object>> listEventsById = getEventById(openEventIds[i], dataSource);
                // if no alarm id in table
                if (listEventsById.isEmpty()) {

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

                con.close();
            }

            return list;

        } catch (Exception e) {
            throw new RuntimeException("Ошибка при операции с базой данных ", e);
        } finally {
            if (con != null)
                con.close();
        }

    }

    private List<HashMap<String, Object>> getOpenEvents(BasicDataSource dataSource) throws SQLException {

        List<HashMap<String, Object>> listOpenedEvents;

        Connection con = null;
        PreparedStatement pstmt;
        ResultSet resultset;

        logger.info(" **** Try to receive Open Events ***** ");
        try {
            con = dataSource.getConnection();

            pstmt = con.prepareStatement("SELECT tt.*, " +
                    "       ttt.unitid, " +
                    "       ttt.nodeid " +
                    "FROM ( " +
                    "       SELECT id, " +
                    "              alarm.neid AS neid, " +
                    "              cast (coalesce(nullif(ne.name, ''), '') AS TEXT) AS ne_name, " +
                    "              eventid, " +
                    "              moi, " +
                    "              perceivedseverity, " +
                    "              alarm.location AS location, " +
                    "              alarmtext, " +
                    "              alarmontime, " +
                    "              cast (coalesce(nullif(alarmofftime, 0), 0) AS INTEGER) AS " +
                    "                alarmofftime, " +
                    "              slotid, " +
                    "              layerid, " +
                    "              subunitid, " +
                    "              unitname, " +
                    "              unit.cfgname AS cfgname, " +
                    "              unit.cfgdescription AS cfgdescription, " +
                    "              cast (coalesce(nullif(unitid, 0), '0') AS INTEGER) AS unitid " +
                    "       FROM public.alarm " +
                    "            LEFT JOIN public.ne ON alarm.neid = ne.neid " +
                    "            LEFT JOIN public.unit ON alarm.slotid = unit.slot AND alarm.neid = " +
                    "              unit.neid AND layer = '0' " +
                    "       WHERE (alarm.alarmofftime IS NULL OR " +
                    "             alarm.alarmofftime = ?) " +
                    "     ) tt " +
                    "     INNER JOIN  " +
                    "     ( " +
                    "       SELECT t1.*, " +
                    "              t2.unitid " +
                    "       FROM ( " +
                    "              SELECT nodeid, " +
                    "                     treehierarchy, " +
                    "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 2), ''), '0') AS INTEGER) AS x , " +
                    "                     cast (coalesce(nullif(split_part(treehierarchy, '.', 3), ''), '0') AS INTEGER) AS y , " +
                    "                     split_part(treehierarchy, '.', 4) AS z, " +
                    "                     typeclass " +
                    "              FROM public.node t1 " +
                    "              WHERE t1.typeclass IN (1, 2) AND " +
                    "                    t1.type <> '' /*and t2.z = ''*/ " +
                    "              ORDER BY t1.nodeid " +
                    "            ) t1 " +
                    "            LEFT JOIN public.unit t2 ON t1.x = t2.neid AND t1.y = t2.slot AND " +
                    "              t2.layer = ? " +
                    "     ) ttt ON tt.unitid = ttt.unitid ");
            pstmt.setInt(1, 0); //alarmofftime
            pstmt.setInt(2, 0); //layer

            logger.debug("DB query: " + pstmt.toString());
            resultset = pstmt.executeQuery();

            listOpenedEvents = convertRStoList(resultset);

            resultset.close();
            pstmt.close();

            con.close();

            logger.debug(" **** Saving Received opened events's IDs and CI IDs ****");

            openEventIds = new String[]{};
            openCiIds = new String[]{};
            if (listOpenedEvents != null) {
                for (HashMap<String, Object> listOpenedEvent : listOpenedEvents) {
                    openEventIds = ArrayUtils.add(openEventIds, listOpenedEvent.get("id").toString());
                    openCiIds = ArrayUtils.add(openCiIds, listOpenedEvent.get("nodeid").toString());
                    logger.debug(" **** Saving ID: " + listOpenedEvent.get("id").toString());
                    logger.debug(" **** Saving CI ID: " + listOpenedEvent.get("nodeid").toString());
                }
            }

            logger.info(" **** Saved " + openEventIds.length + " Opened Events ****");

            return listOpenedEvents;

        } catch (Exception e) {
            throw new RuntimeException("Ошибка при операции с БД", e);
        } finally {
            if (con != null)
                con.close();
        }

    }

    private List<HashMap<String, Object>> getEventById(String id, BasicDataSource dataSource) throws SQLException {

        List<HashMap<String, Object>> list;

        Connection con = null;
        PreparedStatement pstmt;
        ResultSet resultset;

        logger.debug(" **** Try to get Event by ID for " + id);
        try {
            con = dataSource.getConnection();

            pstmt = con.prepareStatement("SELECT  " +
                    "id FROM public.alarm " +
                    "WHERE alarm.id = cast(? AS INTEGER);");
            pstmt.setString(1, id); // id

            logger.debug("DB query: " + pstmt.toString());
            resultset = pstmt.executeQuery();

            list = convertRStoList(resultset);

            resultset.close();
            pstmt.close();

            con.close();

            return list;

        } catch (Exception e) {
            throw new RuntimeException("Ошибка при операции с БД", e);
        } finally {
            if (con != null)
                con.close();
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
            event.setSeverity(Event.PersistentEventSeverity.OK.name());
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
            genErrorMessage("Ошибка конвертирования результата запроса", e);
            return Collections.emptyList();
        }
    }

    private BasicDataSource setupDataSource() {

        String url = String.format("jdbc:postgresql://%s:%s/%s",
                endpoint.getConfiguration().getPostgresqlHost(), endpoint.getConfiguration().getPostgresqlPort(),
                endpoint.getConfiguration().getPostgresqlDb());

        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setMaxIdle(10);
        ds.setMinIdle(5);
        ds.setUsername(endpoint.getConfiguration().getUsername());
        ds.setPassword(endpoint.getConfiguration().getPassword());
        ds.setUrl(url);

        return ds;
    }

    private void closeConnectionsAndDS(BasicDataSource dataSource) {
        try {
            dataSource.close();
        } catch (SQLException e) {
            logger.error("Error while closing Datasource", e);
        }
    }

}