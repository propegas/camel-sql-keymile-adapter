package ru.atc.camel.keymile.events;

import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;

@UriParams
public class KeymileConfiguration {

    private static final int DELAY = 60000;

    private String username;
    private String password;
    private String source;
    private String adaptername;

    @UriParam
    private String postgresqlDb;

    @UriParam
    private String postgresqlHost;

    @UriParam
    private String postgresqlPort;

    @UriParam(defaultValue = "60000")
    private int delay = DELAY;

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

    public String getPostgresqlPort() {
        return postgresqlPort;
    }

    public void setPostgresqlPort(String postgresqlPort) {
        this.postgresqlPort = postgresqlPort;
    }

    public String getPostgresqlHost() {
        return postgresqlHost;
    }

    public void setPostgresqlHost(String postgresqlHost) {
        this.postgresqlHost = postgresqlHost;
    }

    public String getPostgresqlDb() {
        return postgresqlDb;
    }

    public void setPostgresqlDb(String postgresqlDb) {
        this.postgresqlDb = postgresqlDb;
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