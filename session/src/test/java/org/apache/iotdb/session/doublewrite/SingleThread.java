package org.apache.iotdb.session.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class SingleThread implements Runnable {
  private Session session;
  private String deviceId;
  private List<String> measurements;

  public SingleThread(String host, int rpcPort, String deviceId, List<String> measurements) {
    this.session = new Session(host, rpcPort, "root", "root");
    this.deviceId = deviceId;
    this.measurements = measurements;
  }

  @Override
  public void run() {
    try {
      session.open();
      long produceCnt = 0;
      long produceTime = 0;

      List<String> deviceIds = new ArrayList<>();
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

        deviceIds.add(deviceId);
        measurementsList.add(measurements);
        valuesList.add(values);
        typesList.add(types);
        timestamps.add(time);

        if (time != 0 && time % 10 == 0) {
          produceCnt += 1;
          long startTime = System.nanoTime();
          session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
          long endTime = System.nanoTime();
          produceTime += endTime - startTime;
          deviceIds.clear();
          measurementsList.clear();
          valuesList.clear();
          timestamps.clear();
        }
      }

      session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
      session.close();
      System.out.println("Producer: " + (double) produceCnt / (double) produceTime * 1000000000.0 + "/s");
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }
}
