package ru.at_consulting.itsm.device;

import java.io.Serializable;

class Device implements Serializable {

    private static final long serialVersionUID = 1L;
    private String modelNumber;
    private String id;
    private String deviceType;
    private String deviceState;
    private String serialNumber;
    private String commState;
    private String modelName;
    private String location;
    private String hostName;
    private String name;
    private String ipAddress;
    private String parentID;

    @Override
    public String toString() {
        return "Device: " + this.getId() +
                " ParentId: " + this.getParentID() +
                " on host: " + this.getHostName() +
                " name: " + this.getName() +
                " serial number: " + this.getSerialNumber() +
                " location: " + this.getLocation() +
                " device state: " + this.getDeviceState() +
                " comm state: " + this.getCommState();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber;
    }

    public String getCommState() {
        return commState;
    }

    public void setCommState(String commState) {
        this.commState = commState;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getDeviceState() {
        return deviceState;
    }

    public void setDeviceState(String deviceState) {
        this.deviceState = deviceState;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getParentID() {
        return parentID;
    }

    public void setParentID(String parentID) {
        this.parentID = parentID;
    }

    public String getModelNumber() {
        return modelNumber;
    }

    public void setModelNumber(String modelNumber) {
        this.modelNumber = modelNumber;
    }
}
