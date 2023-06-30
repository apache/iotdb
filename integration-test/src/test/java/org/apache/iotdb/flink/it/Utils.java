package org.apache.iotdb.flink.it;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    private static final String deviceId = "root.test.flink.scan";

    protected static void prepareData(String host, int port) throws IoTDBConnectionException, StatementExecutionException {
        Session session = new Session.Builder().host(host).port(port).build();
        session.open(false);
        List<Long> times = new ArrayList<>();
        List<List<String>> measurementsList = new ArrayList<>();
        List<List<TSDataType>> typesList = new ArrayList<>();
        List<List<Object>> valuesList = new ArrayList<>();

        ArrayList<String> measurements =
                new ArrayList<String>() {
                    {
                        for (int i = 0; i < 5; i++) {
                            add(String.format("s%d", i));
                        }
                    }
                };
        ArrayList<TSDataType> types = new ArrayList<TSDataType>() {{
            add(TSDataType.INT32);
            add(TSDataType.INT64);
            add(TSDataType.FLOAT);
            add(TSDataType.DOUBLE);
            add(TSDataType.BOOLEAN);
            add(TSDataType.TEXT);
        }};
        ArrayList<Object> values = new ArrayList<Object>() {{
            add(1);
            add(1L);
            add(1F);
            add(1D);
            add(true);
            add("hello world");
        }};
        for (int i = 0; i < 1000; i++) {
            times.add(Long.valueOf(i));
            measurementsList.add(measurements);
            typesList.add(types);
            valuesList.add(values);
        }
        session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
    }
}
