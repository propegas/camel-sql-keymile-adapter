package ru.atc.camel.keymile.events;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.cache.CacheConstants;
import org.apache.camel.impl.ScheduledPollConsumer;
import org.apache.camel.model.ModelCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thoughtworks.xstream.io.json.JsonWriter.Format;

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
import ru.atc.camel.keymile.events.api.OVMMDevices;
import ru.atc.camel.keymile.events.api.OVMMEvents;

import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
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
	
	private String[] openids = { null };
	
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
        exchange.getIn().setHeader("Source", "OVMM_EVENTS_ADAPTER");

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
		//List<HashMap<String, Object>> listVmStatuses = null;
		int events = 0;
		int statuses = 0;
		try {
			
			logger.info( String.format("***Try to get Open Events***"));
			
			listOpenEvents = getOpenEvents(dataSource);
			
			logger.info( String.format("***Received %d Open Events from SQL***", listOpenEvents.size()));
			String alarmtext, location;
			
			Event genevent = new Event();
			
			//logger.info( String.format("***Try to get VMs statuses***"));
			for(int i=0; i < listOpenEvents.size(); i++) {
			  	
				alarmtext = listOpenEvents.get(i).get("alarmtext").toString();
				location  = listOpenEvents.get(i).get("location").toString();
				logger.debug("DB row " + i + ": " + alarmtext + 
						" " + location);
				
				genevent = genEventObj(listOpenEvents.get(i));
				
				logger.debug("*** Create Exchange ***" );
				
				
				String key = genevent.getHost() + "_" +
						genevent.getObject() + "_" + 
						genevent.getExternalid();
				
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
			
			logger.info( String.format("***Received %d Keymile Open Events from SQL*** ", statuses));
	  
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			dataSource.close();
			return 0;
		}
		catch (NullPointerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
			dataSource.close();
			return 0;
		}
		catch (Error e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error( String.format("Error while get Events from SQL: %s ", e));
			genErrorMessage(e.toString());
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
		
		removeLineFromFile("sendedEvents.dat", "Tgc1-1Cp1_ping_OK");
	
        return 1;
	}
	
	private Event genEventObj(HashMap<String, Object> alarm) {
		// TODO Auto-generated method stub
		Event event;
		
		event = new Event();

		Long eventdate = (long) Integer.parseInt(alarm.get("alarmontime").toString());
		
		event.setTimestamp(eventdate);

		event.setHost(alarm.get("ne_name").toString());
		//event.setCi(vmtitle);
		//vmStatuses.get("ping_colour").toString())
		event.setObject(alarm.get("moi").toString());
		event.setExternalid(alarm.get("id").toString());
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
		event.setStatus("OPEN");
		//event.setService(endpoint.getConfiguration().getSource());
		event.setEventsource(endpoint.getConfiguration().getSource());
		

		//System.out.println(event.toString());
		
		logger.debug(event.toString());
		
		return event;
	}

	private List<HashMap<String, Object>> getOpenEvents(DataSource dataSource) throws SQLException {
		// TODO Auto-generated method stub
        
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
        Connection con = null; 
        PreparedStatement pstmt;
        ResultSet resultset = null;
        try {
        	con = (Connection) dataSource.getConnection();
			//con.setAutoCommit(false);
			
            pstmt = con.prepareStatement("SELECT  " + 
            		"id, alarm.neid as neid, ne.name as ne_name, eventid,  moi, perceivedseverity, alarm.location as location, " +
					" alarmtext, alarmontime, slotid, layerid, subunitid, unitname " +
                    "FROM  public.alarm, public.ne " +
                    "WHERE alarm.neid = ne.neid " +
                    "AND alarm.alarmofftime is null;");
                       // +" LIMIT ?;");
            //pstmt.setString(1, "");
            //pstmt.setInt(2, 3);
            
            logger.debug("DB query: " +  pstmt.toString()); 
            resultset = pstmt.executeQuery();
            //con.commit();
            
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

		} finally {
            if (con != null) con.close();
            
            //return list;
        }
		
	}
	
	private List<HashMap<String, Object>> convertRStoList(ResultSet resultset) throws SQLException {
		
		List<HashMap<String,Object>> list = new ArrayList<HashMap<String,Object>>();
		
		try {
			ResultSetMetaData md = resultset.getMetaData();
	        int columns = md.getColumnCount();
	        //result.getArray(columnIndex)
	        //resultset.get
	        logger.debug("MYSQL columns count: " + columns); 
	        
	        //resultset.last();
	        //int count = resultset.getRow();
	        //logger.debug("MYSQL rows2 count: " + count); 
	        //resultset.beforeFirst();
	        
	        int i = 0, n = 0;
	        //ArrayList<String> arrayList = new ArrayList<String>(); 
	
	        while (resultset.next()) {              
	        	HashMap<String,Object> row = new HashMap<String, Object>(columns);
	            for(int i1=1; i1<=columns; ++i1) {
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
	
	public static String setRightValue(String colour)
	{
		String newstatus = "";
		
		switch (colour) {
        	case "#006600":  newstatus = "OK";break;
        	case "#FF0000":  newstatus = "ERROR";break;
        	default: newstatus = "NA";break;

        	
		}
		/*
		System.out.println("***************** colour: " + colour);
		System.out.println("***************** status: " + newstatus);
		*/
		return newstatus;
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
	
	public void removeLineFromFile(String file, String lineToRemove) {
		BufferedReader br = null;
		PrintWriter pw = null;
	    try {

	      File inFile = new File(file);

	      if (!inFile.isFile()) {
	        System.out.println("Parameter is not an existing file");
	        return;
	      }

	      //Construct the new file that will later be renamed to the original filename.
	      File tempFile = new File(inFile.getAbsolutePath() + ".tmp");

	      br = new BufferedReader(new FileReader(file));
	      pw = new PrintWriter(new FileWriter(tempFile));

	      String line = null;

	      //Read from the original file and write to the new
	      //unless content matches data to be removed.
	      while ((line = br.readLine()) != null) {

	        if (!line.trim().equals(lineToRemove)) {

	          pw.println(line);
	          pw.flush();
	        }
	      }
	      pw.close();
	      br.close();

	      //Delete the original file
	      if (!inFile.delete()) {
	        System.out.println("Could not delete file");
	        return;
	      }

	      //Rename the new file to the filename the original file had.
	      if (!tempFile.renameTo(inFile))
	        System.out.println("Could not rename file");

	    }
	    catch (FileNotFoundException ex) {
	      ex.printStackTrace();
	    }
	    catch (IOException ex) {
	      ex.printStackTrace();
	    }
	    finally {
	    	try {
	    		pw.close();
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	  }

}