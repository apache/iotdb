package org.apache.iotdb.influxdb.protocol.imp;

import org.apache.iotdb.influxdb.IoTDBInfluxDBUtils;
import org.apache.iotdb.influxdb.protocol.cache.DatabaseCache;
import org.apache.iotdb.influxdb.protocol.constant.InfluxDBConstant;
import org.apache.iotdb.influxdb.protocol.dto.IoTDBRecord;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;

import org.influxdb.InfluxDBException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBInfluxDBService {
    private final Session session;
    private DatabaseCache databaseCache;

    public IoTDBInfluxDBService(Session session) {
        this.session = session;
        this.databaseCache = new DatabaseCache();
    }

    public void setDatabase(String database) {
        if (!database.equals(this.databaseCache.getDatabase())) {
            updateDatabase(database);
        }
    }

    private void writePoint(Point point) {
        String measurement = null;
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        Long time = null;
        java.lang.reflect.Field[] reflectFields = point.getClass().getDeclaredFields();
        // Get the property of point in influxdb by reflection
        for (java.lang.reflect.Field reflectField : reflectFields) {
            reflectField.setAccessible(true);
            try {
                if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map")
                        && reflectField.getName().equalsIgnoreCase("fields")) {
                    fields = (Map<String, Object>) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map")
                        && reflectField.getName().equalsIgnoreCase("tags")) {
                    tags = (Map<String, String>) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.lang.String")
                        && reflectField.getName().equalsIgnoreCase("measurement")) {
                    measurement = (String) reflectField.get(point);
                }
                if (reflectField.getType().getName().equalsIgnoreCase("java.lang.Number")
                        && reflectField.getName().equalsIgnoreCase("time")) {
                    time = (Long) reflectField.get(point);
                }
                // set current time
                if (time == null) {
                    time = System.currentTimeMillis();
                }
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
        IoTDBInfluxDBUtils.checkNonEmptyString(measurement, "measurement name");

        String path = generatePath(measurement, tags);
        insertRecord(path, time, fields);
    }

    private String generatePath(String measurement, Map<String, String> tags) {
        String database = this.databaseCache.getDatabase();
        Map<String, Map<String, Integer>> measurementTagOrder =
                this.databaseCache.getMeasurementTagOrder();
        Map<String, Integer> tagOrders = measurementTagOrder.get(measurement);
        if (tagOrders == null) {
            tagOrders = new HashMap<>();
        }
        int measurementTagNum = tagOrders.size();
        // The actual number of tags at the time of current insertion
        Map<Integer, String> realTagOrders = new HashMap<>();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (tagOrders.containsKey(entry.getKey())) {
                realTagOrders.put(tagOrders.get(entry.getKey()), entry.getKey());
            } else {
                measurementTagNum++;
                try {
                    updateNewTagIntoDB(measurement, entry.getKey(), measurementTagNum, database);
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    throw new InfluxDBException(e.getMessage());
                }
                realTagOrders.put(measurementTagNum, entry.getKey());
                tagOrders.put(entry.getKey(), measurementTagNum);
            }
        }
        // update tagOrder map in memory
        measurementTagOrder.put(measurement, tagOrders);
        this.databaseCache.setMeasurementTagOrder(measurementTagOrder);
        StringBuilder path = new StringBuilder("root." + database + "." + measurement);
        for (int i = 1; i <= measurementTagNum; i++) {
            if (realTagOrders.containsKey(i)) {
                path.append(".").append(tags.get(realTagOrders.get(i)));
            } else {
                path.append("." + InfluxDBConstant.placeholder);
            }
        }
        return path.toString();
    }

    /**
     * When a new tag appears, it is inserted into the database
     *
     * @param measurement inserted measurement
     * @param tag         tag name
     * @param order       tag order
     * @param database    inserted database
     */
    private void updateNewTagIntoDB(String measurement, String tag, int order, String database)
            throws IoTDBConnectionException, StatementExecutionException {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        measurements.add("database_name");
        measurements.add("measurement_name");
        measurements.add("tag_name");
        measurements.add("tag_order");
        types.add(TSDataType.TEXT);
        types.add(TSDataType.TEXT);
        types.add(TSDataType.TEXT);
        types.add(TSDataType.INT32);
        values.add(database);
        values.add(measurement);
        values.add(tag);
        values.add(order);
        session.insertRecord("root.TAG_INFO", System.currentTimeMillis(), measurements, types, values);
    }

    private void insertRecord(String path, Long time, Map<String, Object> fields) {
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            measurements.add(entry.getKey());
            Object value = entry.getValue();
            types.add(IoTDBInfluxDBUtils.normalTypeToTSDataType(value));
            values.add(value);
        }
        try {
            session.insertRecord(String.valueOf(path), time, measurements, types, values);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new InfluxDBException(e.getMessage());
        }
    }

    public void writePoints(
            String database,
            String retentionPolicy,
            String precision,
            String consistency,
            BatchPoints batchPoints) {
        if (database != null && !database.equals(databaseCache.getDatabase())) {
            updateDatabase(database);
        }
        List<String> deviceIds = new ArrayList<>();
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();
        for (Point point : batchPoints.getPoints()) {
            IoTDBRecord ioTDBRecord = generatePointRecord(point);
            deviceIds.add(ioTDBRecord.getDeviceId());
            times.add(ioTDBRecord.getTime());
            measurementsList.add(ioTDBRecord.getMeasurements());
            valuesList.add(ioTDBRecord.getValues());
            deviceIds.add(ioTDBRecord.getDeviceId());
            writePoint(point);
        }
    }

    private IoTDBRecord generatePointRecord(Point point) {
        return null;
    }

    /**
     * when the database changes, update the database related information, that is, obtain the list
     * and order of all tags corresponding to the database from iotdb
     *
     * @param database update database name
     */
    private void updateDatabase(String database) {
        this.databaseCache.setDatabase(database);
        try {
            SessionDataSet result =
                    session.executeQueryStatement(
                            "select * from root.TAG_INFO where database_name="
                                    + String.format("\"%s\"", database));
            Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();
            while (result.hasNext()) {
                List<Field> fields = result.next().getFields();
                String measurementName = fields.get(1).getStringValue();
                Map<String, Integer> tagOrder;
                if (measurementTagOrder.containsKey(measurementName)) {
                    tagOrder = measurementTagOrder.get(measurementName);
                } else {
                    tagOrder = new HashMap<>();
                }
                tagOrder.put(fields.get(2).getStringValue(), fields.get(3).getIntV());
                measurementTagOrder.put(measurementName, tagOrder);
            }
            this.databaseCache.setMeasurementTagOrder(measurementTagOrder);
        } catch (StatementExecutionException e) {
            // at first execution, tag_INFO table is not created, intercept the error
            if (e.getStatusCode() != 411) {
                throw new InfluxDBException(e.getMessage());
            }
        } catch (IoTDBConnectionException e) {
            throw new InfluxDBException(e.getMessage());
        }
    }
}
