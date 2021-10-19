package org.apache.iotdb.influxdb.protocol.dto;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class IoTDBRecord {
    private String deviceId;
    private long time;
    private List<String> measurements;
    private List<TSDataType> types;
    private List<Object> values;

    public IoTDBRecord(){}

    public IoTDBRecord(String deviceId, long time, List<String> measurements, List<TSDataType> types, List<Object> values) {
        this.deviceId = deviceId;
        this.time = time;
        this.measurements = measurements;
        this.types = types;
        this.values = values;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public List<String> getMeasurements() {
        return measurements;
    }

    public void setMeasurements(List<String> measurements) {
        this.measurements = measurements;
    }

    public List<TSDataType> getTypes() {
        return types;
    }

    public void setTypes(List<TSDataType> types) {
        this.types = types;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }
}
