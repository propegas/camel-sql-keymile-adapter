package ru.atc.camel.keymile.events;

//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

//import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
//import org.apache.camel.Producer;
//import org.apache.camel.ProducerTemplate;
//import org.apache.camel.component.cache.CacheConstants;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.camel.model.ModelCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.thoughtworks.xstream.io.json.JsonWriter.Format;

//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarmSeverity;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCAlarm;
//import com.apc.stdws.xsd.isxcentral._2009._10.ISXCDevice;
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;

//import ru.at_consulting.itsm.device.Device;
import ru.at_consulting.itsm.event.Event;
//import ru.atc.camel.keymile.events.api.OVMMDevices;
//import ru.atc.camel.keymile.events.api.OVMMEvents;

import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

//import com.mysql.jdbc.Connection;
//import com.mysql.jdbc.Driver;
import org.postgresql.Driver;



public class KeymileConsumer extends ScheduledPollConsumer {
	
	private String[] openids = {  };
	
	private static Logger logger = LoggerFactory.getLogger(Main.class);
	
	public static KeymileEndpoint endpoint;
	
	public static ModelCamelContext context;
	
	public enum PersistentEventSeverity {
	    OK, INFO, WARNING, MINOR, MAJOR, CRITICAL;
		
	    public String value() {
	        return name();
	    }

	    public static PersistentEventSeverity fromValue(String v) {
	        return valueOf(v);
	    }
	}

	public KeymileConsumer(KeymileEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        //this.afterPoll();
        this.setTimeUnit(TimeUnit.MINUTES);
        this.setInitialDelay(0);
        this.setDelay(endpoint.getConfiguration().getDelay());
	}
	
	public static ModelCamelContext getContext() {
		// TODO Auto-generated method stub
				return context;
	}
	
	public static void setContext(ModelCamelContext context1){
		context = context1;

	}

	@Override
	protected int poll() throws Exception {
		
		String operationPath = endpoint.getOperationPath();
		
		if (operationPath.equals("events")) return processSearchEvents();
		
		// only one operation implemented for now !
		throw new IllegalArgumentException("Incorrect operation: " + operationPath);
	}
	
	@Override
	public long beforePoll(long timeout) throws Exception {
		
		logger.info("*** Before Poll!!!");
		// only one operation implemented for now !
		//throw new IllegalArgumentException("Incorrect operation: ");
		
		//send HEARTBEAT
		//genHeartbeatMessage();
		
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
        
        exchange.getIn().setHeader("EventIdAndStatus", "Error_" +timestamp);
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

	
	public static void genHeartbeatMessage(Exchange exchange) {
		// TODO Auto-generated method stub
		long timestamp = System.currentTimeMillis();
		timestamp = timestamp / 1000;
		//String textError = "Возникла ошибка при работе адаптера: ";
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

        try {
        	//Processor processor = getProcessor();
        	//.process(exchange);
        	//processor.process(exchange);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		} 
	}
	
	
	// "throws Exception" 
	private int processSearchEvents()  throws Exception, Error, SQLException {
		
		//Long timestamp;
		BasicDataSource dataSource = setupDataSource();
		
		List<HashMap<String, Object>> listOpenEvents = new ArrayList<HashMap<String,Object>>();
		List<HashMap<String, Object>> listClosedEvents = new ArrayList<HashMap<String,Object>>();
		//List<HashMap<String, Object>> listVmStatuses = null;
		int events = 0;
		//int statuses = 0;
		try {
			
			// get All Closed events
			if (openids.length != 0) {
				logger.info( String.format("***Try to get Closed Events***"));
				listClosedEvents = getClosedEvents(dataSource);
				logger.info( String.format("***Received %d Closed Events from SQL***", listClosedEvents.size()));
			}
			// get All new (Open) events
			logger.info( String.format("***Try to get Open Events***"));
			listOpenEvents = getOpenEvents(dataSource);
			logger.info( String.format("***Received %d Open Events from SQL***", listOpenEvents.size()));
			
			List<HashMap<String, Object>> listAllEvents = new ArrayList<HashMap<String,Object>>();
			listAllEvents.addAll(listOpenEvents);
			listAllEvents.addAll(listClosedEvents);
			
			// List<HashMap<String, Object>> allevents = (List<HashMap<String, Object>>) ArrayUtils.addAll(listOpenEvents);
			
			String alarmtext, location;
			
			Event genevent = new Event();
			
			//logger.info( String.format("***Try to get VMs statuses***"));
			for(int i=0; i < listAllEvents.size(); i++) {
			  	
				/*
				alarmtext = listAllEvents.get(i).get("alarmtext").toString();
				location  = listAllEvents.get(i).get("location").toString();
				logger.debug("DB row " + i + ": " + alarmtext + 
						" " + location);
				*/
				
				genevent = genEventObj(listAllEvents.get(i), dataSource);
				
				logger.debug("*** Create Exchange ***" );

				String key = genevent.getExternalid() + "_" + 
						genevent.getStatus();
				
				Exchange exchange = getEndpoint().createExchange();
				exchange.getIn().setBody(genevent, Event.class);
				exchange.getIn().setHeader("EventUniqId", key);
				
				exchange.getIn().setHeader("Object", genevent.getObject());
				exchange.getIn().setHeader("Timestamp", genevent.getTimestamp());
				
				
				//exchange.getIn().setHeader("DeviceType", vmevents.get(i).getDeviceType());

				try {
					getProcessor().process(exchange);
					events++;
					
					//File cachefile = new File("sendedEvents.dat");
					//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error( String.format("Error while process Exchange message: %s ", e));
				} 
						
					
			}
			
			logger.debug( String.format("***Received %d Keymile Open Events from SQL*** ", listOpenEvents.size()));
			logger.debug( String.format("***Received %d Keymile Closed Events from SQL*** ", listClosedEvents.size()));
			
           // logger.info(" **** Received " + events.length + " Opened Events ****");
    		
    		
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		catch (Error e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.getMessage() + " " + e.toString());
			dataSource.close();
			return 0;
		}
		finally
		{
			dataSource.close();
			//return 0;
		}
		
		dataSource.close();
		
		logger.info( String.format("***Sended to Exchange messages: %d ***", events));
		
		//removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
	
        return 1;
	}
	
	private List<HashMap<String, Object>> getClosedEvents(BasicDataSource dataSource) throws SQLException, Throwable {
		// TODO Auto-generated method stub
		
		int event_count = 0;
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		List<HashMap<String,Object>> listc = new ArrayList<HashMap<String,Object>>();
		Connection con = null; 
	    PreparedStatement pstmt;
	    ResultSet resultset = null;
	    
	    logger.info(" **** Try to receive Closed Events ***** " );
	    try {
	    	for(int i=0; i < openids.length; i++){
	    		
	    		logger.debug(" **** Try to receive Closed Event for " + openids[i] );
		    	con = (Connection) dataSource.getConnection();
				//con.setAutoCommit(false);
				
		        pstmt = con.prepareStatement("SELECT  " + 
		        		"id, cast(coalesce(nullif(ne.name, ''), '') as text) as ne_name, "
		        		+ "eventid,  moi, perceivedseverity, alarm.location as location, " +
						" alarmtext, alarmontime, cast(coalesce(nullif(alarmofftime, 0),'0') as integer) as alarmofftime, slotid, layerid, subunitid, unitname, " +
						" unit.cfgname as cfgname,  unit.cfgdescription as cfgdescription,  "
						+ " cast(coalesce(nullif(unitid, 0),'0') as integer) as unitid " + 
						"FROM  public.alarm left join public.ne " +
		                "on alarm.neid = ne.neid left join public.unit " +
		                "on alarm.slotid = unit.slot  " +
		                "AND alarm.neid = unit.neid " + 
		                "AND layer = ? " +
		                "WHERE (alarm.alarmofftime is not null and  " +
		                " alarm.alarmofftime != ?) "
		                + "AND alarm.id = cast(? as INTEGER) ;");
		                   // +" LIMIT ?;");

		        //pstmt.setString(1, "");
		        pstmt.setString(3, openids[i]); // id
		        pstmt.setInt(1, 0); // layer
		        pstmt.setInt(2, 0); // alarmofftime
		        
		        logger.debug("DB query: " +  pstmt.toString()); 
		        resultset = pstmt.executeQuery();
		        //con.commit();
		        
		        listc = convertRStoList(resultset);
		        list.addAll(listc);
		        
		        listc = getEventById(openids[i], dataSource);
		        // if no alarm id in table
		        if (listc.size() == 0 ){
		        	
		        	logger.info("DB alarm: " +  openids[i] + " not found"); 
		        	int columns = 2;
		        	HashMap<String,Object> row = new HashMap<String, Object>(columns);
		     
		        	row.put("id",openids[i]);
		        	
		        	logger.debug("Put \"closed\" mark to hash"); 
		        	row.put("closed","1");

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
			logger.error( String.format("Error while SQL execution: %s ", e));
			
			if (con != null) con.close();
			
			//return null;
			throw e;
	
		} catch (Throwable e) { //send error message to the same queue
			// TODO Auto-generated catch block
			logger.error( String.format("Error while execution: %s ", e));
			//genErrorMessage(e.getMessage());
			// 0;
			throw e;
		} finally {
	        if (con != null) con.close();
	        
	        //return list;
	    }
		
	}

	private List<HashMap<String, Object>> getOpenEvents(BasicDataSource dataSource) throws SQLException, Throwable {
		// TODO Auto-generated method stub
	    
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
	    Connection con = null; 
	    PreparedStatement pstmt;
	    ResultSet resultset = null;
	    
	    logger.info(" **** Try to receive Open Events ***** " );
	    try {
	    	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
			
	        pstmt = con.prepareStatement("SELECT  " + 
	        		"id, alarm.neid as neid, cast(coalesce(nullif(ne.name, ''), '') as text) as ne_name, "
	        		+ " eventid,  moi, perceivedseverity, alarm.location as location, " +
					" alarmtext, alarmontime, cast(coalesce(nullif(alarmofftime, 0),'0') as integer) as alarmofftime, slotid, layerid, subunitid, unitname, " +
	                " unit.cfgname as cfgname,  unit.cfgdescription as cfgdescription,  "
	                + " cast(coalesce(nullif(unitid, 0),'0') as integer) as unitid " + 
					"FROM  public.alarm left join public.ne " +
	                "on alarm.neid = ne.neid left join public.unit " +
	                "on alarm.slotid = unit.slot  " +
	                "AND alarm.neid = unit.neid " + 
	                "AND layer = ? " +
	                " WHERE (alarm.alarmofftime is null OR  " +
	                " alarm.alarmofftime = ?);");
	                   // +" LIMIT ?;");
	        //pstmt.setString(1, "");
	        pstmt.setInt(1, 0);
	        pstmt.setInt(2, 0);
	        
	        logger.debug("DB query: " +  pstmt.toString()); 
	        resultset = pstmt.executeQuery();
	        //resultset.get
	        //con.commit();
	        
	        list = convertRStoList(resultset);
	        
	        
	        resultset.close();
	        pstmt.close();
	        
	        if (con != null) con.close();
	        
	        logger.debug(" **** Saving Received opened events's IDs ****");
			
			openids = new String[]{ };
			for(int i=0; i < list.size(); i++){
				openids = (String[]) ArrayUtils.add(openids,list.get(i).get("id").toString());
				logger.debug(" **** Saving ID: " + list.get(i).get("id").toString());
			}
			
			logger.info(" **** Saved " + openids.length + " Opened Events ****");
	
	        
	        return list;
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.error( String.format("Error while SQL execution: %s ", e));
			
			if (con != null) con.close();
			
			//return null;
			throw e;
	
		} catch (Throwable e) { //send error message to the same queue
			// TODO Auto-generated catch block
			logger.error( String.format("Error while execution: %s ", e));
			//genErrorMessage(e.getMessage());
			// 0;
			throw e;
		} finally {
	        if (con != null) con.close();
	        
	        //return list;
	    }
	    
	
		
	}
	
	private List<HashMap<String, Object>> getEventById(String id, BasicDataSource dataSource) throws SQLException, Throwable {
		// TODO Auto-generated method stub
	    
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
	    Connection con = null; 
	    PreparedStatement pstmt;
	    ResultSet resultset = null;
	    
	    logger.debug(" **** Try to get Event by ID for " + id );
	    try {
	    	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
			
	        pstmt = con.prepareStatement("SELECT  " + 
	        		"id FROM public.alarm " +
	                "WHERE alarm.id = cast(? as INTEGER);");
	                   // +" LIMIT ?;");
	        //pstmt.setString(1, "");
	        pstmt.setString(1, id); // id
	        
	        logger.debug("DB query: " +  pstmt.toString()); 
	        resultset = pstmt.executeQuery();
	        
	        list = convertRStoList(resultset);
	        
	        resultset.close();
	        pstmt.close();
	        
	        if (con != null) con.close();
	        
	        return list;
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			logger.error( String.format("Error while SQL execution: %s ", e));
			
			if (con != null) con.close();
			
			//return null;
			throw e;
	
		} catch (Throwable e) { //send error message to the same queue
			// TODO Auto-generated catch block
			logger.error( String.format("Error while execution: %s ", e));
			//genErrorMessage(e.getMessage());
			// 0;
			throw e;
		} finally {
	        if (con != null) con.close();
	        
	        //return list;
	    }
	    
	
		
	}

	private Event genEventObj(HashMap<String, Object> alarm, BasicDataSource dataSource) throws SQLException, Throwable {
		// TODO Auto-generated method stub
		Event event;
		boolean oldeventclosed = false;
		List<HashMap<String,Object>> evlist = new ArrayList<HashMap<String,Object>>();
		
		event = new Event();
		
		logger.debug( String.format("ID: %s ", alarm.get("id").toString()));
		
		//evlist = getEventById(alarm.get("id").toString(), dataSource);
		if (alarm.containsKey("closed")) {
			
			logger.debug("Has \"closed\" mark in hash"); 
			long timestamp = System.currentTimeMillis();
			timestamp = timestamp / 1000;
			oldeventclosed = true;
			event.setHost("");
			event.setExternalid(alarm.get("id").toString());
			event.setTimestamp(timestamp);
			event.setSeverity( PersistentEventSeverity.OK.name());
			event.setStatus("CLOSED");
			event.setMessage(String.format("Ошибка на оборудовании Keymile устранена"));
			
		}
		else {
			Long eventclosedate = (long) 0, eventopendate = (long) 0;
			eventopendate = (long) Integer.parseInt(alarm.get("alarmontime").toString());
			eventclosedate = (long) Integer.parseInt(alarm.get("alarmofftime").toString());
			
			logger.debug( String.format("alarmofftime: %s, %d ", alarm.get("alarmofftime").toString(), eventclosedate ));
			
			event.setHost(alarm.get("ne_name").toString());
			//event.setCi(vmtitle);
			//vmStatuses.get("ping_colour").toString())
			event.setObject(alarm.get("moi").toString());
			event.setExternalid(alarm.get("id").toString());
			event.setCi(String.format("%s",alarm.get("unitid").toString()));
			//event.setParametr("Status");
			//String status = "OPEN";
			//event.setParametrValue(status);
			
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
			}
			else {
				event.setTimestamp(eventopendate);
				event.setStatus("OPEN");
			}
		}
		
		//event.setService(endpoint.getConfiguration().getSource());
		event.setEventsource(endpoint.getConfiguration().getSource());
		

		//System.out.println(event.toString());
		
		logger.debug(event.toString());
		
		return event;
	}

	private List<HashMap<String, Object>> convertRStoList(ResultSet resultset) throws SQLException {
		
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
		try {
			ResultSetMetaData md = resultset.getMetaData();
	        int columns = md.getColumnCount();
	        //result.getArray(columnIndex)
	        //resultset.get
	        logger.debug("DB SQL columns count: " + columns); 
	        
	        //resultset.last();
	        //int count = resultset.getRow();
	        //logger.debug("MYSQL rows2 count: " + count); 
	        //resultset.beforeFirst();
	        
	        int i = 0, n = 0;
	        //ArrayList<String> arrayList = new ArrayList<String>(); 
	
	        while (resultset.next()) {              
	        	HashMap<String,Object> row = new HashMap<String, Object>(columns);
	            for(int i1=1; i1<=columns; ++i1) {
	            	logger.debug("DB SQL getColumnLabel: " + md.getColumnLabel(i1)); 
	            	logger.debug("DB SQL getObject: " + resultset.getObject(i1)); 
	                row.put(md.getColumnLabel(i1),resultset.getObject(i1));
	            }
	            list.add(row);                 
	        }
	        
	        return list;
	        
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
			return null;

		} finally {

		}
	}

	private BasicDataSource setupDataSource() {
		
		String url = String.format("jdbc:postgresql://%s:%s/%s",
		endpoint.getConfiguration().getPostgresql_host(), endpoint.getConfiguration().getPostgresql_port(),
		endpoint.getConfiguration().getPostgresql_db());
		
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.postgresql.Driver");
        ds.setUsername( endpoint.getConfiguration().getUsername() );
        ds.setPassword( endpoint.getConfiguration().getPassword() );
        ds.setUrl(url);
              
        return ds;
    }
	
	
	public static String setRightSeverity(String severity)
	{
		String newseverity = "";
			
		switch (severity) {
        	case "0":  newseverity = PersistentEventSeverity.CRITICAL.name();break;
        	case "1":  newseverity = PersistentEventSeverity.MAJOR.name();break;
        	case "2":  newseverity = PersistentEventSeverity.MINOR.name();break;
        	case "3":  newseverity = PersistentEventSeverity.INFO.name();break;
        	default: newseverity = PersistentEventSeverity.INFO.name();break;

        	
		}
		/*
		System.out.println("***************** colour: " + colour);
		System.out.println("***************** newseverity: " + newseverity);
		*/
		return newseverity;
	}
	
	
}