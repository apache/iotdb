package org.apache.iotdb.db.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class MultiThreadTest {
  private static ArrayList<Session> sessions = new ArrayList<>();
  private static ArrayList<String> deviceIds = new ArrayList<>();

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < 10; i++) {
      Session session = new Session("127.0.0.1", 6668, "root", "root");
      session.open();
      sessions.add(session);
      String deviceId = "root.dw.d0" + (i / 2 + 1);
      deviceIds.add(deviceId);
    }

    testInsertRecords();

    for (Session session : sessions) {
      session.close();
    }
  }

  public static void testInsertRecords()
      throws IoTDBConnectionException, StatementExecutionException {
    long produceCnt = 0;
    long produceTime = 0;

    List<String> measurements = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    List<List<String>> curDeviceIds = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      curDeviceIds.add(new ArrayList<>());
    }
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();

    for (long time = 0; time < 1000000; time++) {
      List<Object> values = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);
      types.add(TSDataType.INT64);

      measurementsList.add(measurements);
      valuesList.add(values);
      typesList.add(types);
      timestamps.add(time);

      for (int i = 0; i < 10; i++) {
        curDeviceIds.get(i).add(deviceIds.get(i));
      }

      if (time != 0 && time % 10 == 0) {
        produceCnt += 10;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
          sessions
              .get(i)
              .insertRecords(
                  curDeviceIds.get(i), timestamps, measurementsList, typesList, valuesList);
        }
        long endTime = System.currentTimeMillis();
        produceTime += endTime - startTime;
        measurementsList.clear();
        valuesList.clear();
        timestamps.clear();
        curDeviceIds.clear();
        for (int i = 0; i < 10; i++) {
          curDeviceIds.add(new ArrayList<>());
        }
      }
    }

    // session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
    System.out.println("Producer: " + (double) produceCnt / (double) produceTime * 1000.0 + "/s");
  }
}
