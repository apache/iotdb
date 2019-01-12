package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * TSFileMetaData collects all metadata info and saves in its data structure
 */
public class TsFileMetaData {

    private Map<String, TsDeviceMetadataIndex> deviceIndexMap = new HashMap<>();

    /**
     * TSFile schema for this file. This schema contains metadata for all the time series.
     */
    private Map<String, MeasurementSchema> measurementSchema = new HashMap<>();

    /**
     * Version of this file
     */
    private int currentVersion;

    /**
     * String for application that wrote this file. This should be in the format <Application> version
     * <App Version> (build <App Build Hash>). e.g. impala version 1.0 (build SHA-1_hash_code)
     */
    private String createdBy;


    public TsFileMetaData() {
    }

    /**
     * @param measurementSchema - time series info list
     * @param currentVersion    - current version
     */
    public TsFileMetaData(Map<String, TsDeviceMetadataIndex> deviceMap, Map<String, MeasurementSchema> measurementSchema, int currentVersion) {
        this.deviceIndexMap = deviceMap;
        this.measurementSchema = measurementSchema;
        this.currentVersion = currentVersion;
    }

    /**
     * add time series metadata to list. THREAD NOT SAFE
     *
     * @param measurementSchema series metadata to add
     */
    public void addMeasurementSchema(MeasurementSchema measurementSchema) {
        this.measurementSchema.put(measurementSchema.getMeasurementId(), measurementSchema);
    }

    @Override
    public String toString() {
        return "TsFileMetaData{" +
                "deviceIndexMap=" + deviceIndexMap +
                ", measurementSchema=" + measurementSchema +
                ", currentVersion=" + currentVersion +
                ", createdBy='" + createdBy + '\'' +
                '}';
    }

    public int getCurrentVersion() {
        return currentVersion;
    }

    public void setCurrentVersion(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Map<String, TsDeviceMetadataIndex> getDeviceMap() {
        return deviceIndexMap;
    }

    public void setDeviceMap(Map<String, TsDeviceMetadataIndex> deviceMap) {
        this.deviceIndexMap = deviceMap;
    }

    public boolean containsDevice(String DeltaObjUID) {
        return this.deviceIndexMap.containsKey(DeltaObjUID);
    }

    public TsDeviceMetadataIndex getDeviceMetadataIndex(String DeltaObjUID) {
        return this.deviceIndexMap.get(DeltaObjUID);
    }

    public boolean containsMeasurement(String measurement) {
        return measurementSchema.containsKey(measurement);
    }


    public TSDataType getType(String measurement) {
        if (containsMeasurement(measurement))
            return measurementSchema.get(measurement).getType();
        else
            return null;
    }


    public Map<String, MeasurementSchema> getMeasurementSchema() {
        return measurementSchema;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(deviceIndexMap.size(), outputStream);
        for (Map.Entry<String, TsDeviceMetadataIndex> entry : deviceIndexMap.entrySet()) {
            byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
            byteLen += entry.getValue().serializeTo(outputStream);
        }

        byteLen += ReadWriteIOUtils.write(measurementSchema.size(), outputStream);
        for (Map.Entry<String, MeasurementSchema> entry : measurementSchema.entrySet()) {
            byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
            byteLen += entry.getValue().serializeTo(outputStream);
        }

        byteLen += ReadWriteIOUtils.write(currentVersion, outputStream);

        byteLen += ReadWriteIOUtils.writeIsNull(createdBy, outputStream);
        if (createdBy != null) byteLen += ReadWriteIOUtils.write(createdBy, outputStream);

        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        int byteLen = 0;

        byteLen += ReadWriteIOUtils.write(deviceIndexMap.size(), buffer);
        for (Map.Entry<String, TsDeviceMetadataIndex> entry : deviceIndexMap.entrySet()) {
            byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
            byteLen += entry.getValue().serializeTo(buffer);
        }

        byteLen += ReadWriteIOUtils.write(measurementSchema.size(), buffer);
        for (Map.Entry<String, MeasurementSchema> entry : measurementSchema.entrySet()) {
            byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
            byteLen += entry.getValue().serializeTo(buffer);
        }

        byteLen += ReadWriteIOUtils.write(currentVersion, buffer);

        byteLen += ReadWriteIOUtils.writeIsNull(createdBy, buffer);
        if (createdBy != null) byteLen += ReadWriteIOUtils.write(createdBy, buffer);

        return byteLen;
    }

    public static TsFileMetaData deserializeFrom(InputStream inputStream) throws IOException {
        TsFileMetaData fileMetaData = new TsFileMetaData();

        int size = ReadWriteIOUtils.readInt(inputStream);
        if (size > 0) {
            Map<String, TsDeviceMetadataIndex> deviceMap = new HashMap<>();
            String key;
            TsDeviceMetadataIndex value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(inputStream);
                value = TsDeviceMetadataIndex.deserializeFrom(inputStream);
                deviceMap.put(key, value);
            }
            fileMetaData.deviceIndexMap = deviceMap;
        }

        size = ReadWriteIOUtils.readInt(inputStream);
        if (size > 0) {
            fileMetaData.measurementSchema = new HashMap<>();
            String key;
            MeasurementSchema value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(inputStream);
                value = MeasurementSchema.deserializeFrom(inputStream);
                fileMetaData.measurementSchema.put(key, value);
            }
        }

        fileMetaData.currentVersion = ReadWriteIOUtils.readInt(inputStream);

        if (ReadWriteIOUtils.readIsNull(inputStream))
            fileMetaData.createdBy = ReadWriteIOUtils.readString(inputStream);

        return fileMetaData;
    }

    public static TsFileMetaData deserializeFrom(ByteBuffer buffer) throws IOException {
        TsFileMetaData fileMetaData = new TsFileMetaData();

        int size = ReadWriteIOUtils.readInt(buffer);
        if (size > 0) {
            Map<String, TsDeviceMetadataIndex> deviceMap = new HashMap<>();
            String key;
            TsDeviceMetadataIndex value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(buffer);
                value = TsDeviceMetadataIndex.deserializeFrom(buffer);
                deviceMap.put(key, value);
            }
            fileMetaData.deviceIndexMap = deviceMap;
        }

        size = ReadWriteIOUtils.readInt(buffer);
        if (size > 0) {
            fileMetaData.measurementSchema = new HashMap<>();
            String key;
            MeasurementSchema value;
            for (int i = 0; i < size; i++) {
                key = ReadWriteIOUtils.readString(buffer);
                value = MeasurementSchema.deserializeFrom(buffer);
                fileMetaData.measurementSchema.put(key, value);
            }
        }

        fileMetaData.currentVersion = ReadWriteIOUtils.readInt(buffer);

        if (ReadWriteIOUtils.readIsNull(buffer))
            fileMetaData.createdBy = ReadWriteIOUtils.readString(buffer);

        return fileMetaData;
    }

}
