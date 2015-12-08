package ru.atc.camel.keymile.events;

import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

@UriParams
public class KeymileConfiguration {	
    
	private String username;
	
	private String password;
	
	private String source;
	
	private String adaptername;
	
	
	@UriParam
    private String postgresql_db;
    
    @UriParam
    private String postgresql_host;
    
    @UriParam
    private String table_prefix;
    
    public String getTable_prefix() {
		return table_prefix;
	}

	public void setTable_prefix(String table_prefix) {
		this.table_prefix = table_prefix;
	}

	@UriParam
    private String postgresql_port;
    
    @UriParam
    private String query;
    
    @UriParam(defaultValue = "60000")
    private int delay = 60000;

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPostgresql_port() {
		return postgresql_port;
	}

	public void setPostgresql_port(String postgresql_port) {
		this.postgresql_port = postgresql_port;
	}

	public String getPostgresql_host() {
		return postgresql_host;
	}

	public void setPostgresql_host(String postgresql_host) {
		this.postgresql_host = postgresql_host;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getPostgresql_db() {
		return postgresql_db;
	}

	public void setPostgresql_db(String postgresql_db) {
		this.postgresql_db = postgresql_db;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getAdaptername() {
		return adaptername;
	}

	public void setAdaptername(String adaptername) {
		this.adaptername = adaptername;
	}


}