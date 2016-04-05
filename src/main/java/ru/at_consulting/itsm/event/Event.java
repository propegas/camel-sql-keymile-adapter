package ru.at_consulting.itsm.event;

import java.io.Serializable;

public class Event implements Serializable {

    private static final long serialVersionUID = 1L;
    private String host;
    private String object;
    private String parametr;
    private String service;
    private String category;
    private Integer uuid;
    private Long timestamp;
    private String severity;
    private String message;
    private String status;
    private String externalid;
    private String ci;
    private String origin;
    private String eventCategory;
    private String module;
    private String eventsource;
    private Integer repeatCounter = 0;

    @Override
    public String toString() {
        return "Message: " + this.getMessage() +
                " on host: " + this.getHost() +
                " with severity: " + this.getSeverity() +
                " object: " + this.getObject() +
                " parameter: " + this.getParametr() +
                " and status: " + this.getStatus();
    }

    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getService() {
        return this.service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public Integer getUuid() {
        return uuid;
    }

    public void setUuid(Integer uuid) {
        this.uuid = uuid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getParametr() {
        return parametr;
    }

    public void setParametr(String parametr) {
        this.parametr = parametr;
    }

    public String getExternalid() {
        return externalid;
    }

    public void setExternalid(String externalid) {
        this.externalid = externalid;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }

    public String getCi() {
        return ci;
    }

    public void setCi(String ci) {
        this.ci = ci;
    }

    public String getEventsource() {
        return eventsource;
    }

    public void setEventsource(String eventsource) {
        this.eventsource = eventsource;
    }

    public int getRepeatCounter() {
        return repeatCounter;
    }

    public void setRepeatCounter(Integer repeatCounter) {
        this.repeatCounter = repeatCounter;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getEventCategory() {
        return eventCategory;
    }

    public void setEventCategory(String eventCategory) {
        this.eventCategory = eventCategory;
    }

    public String getModule() {
        return module;
    }

    public void setModule(String module) {
        this.module = module;
    }

}
