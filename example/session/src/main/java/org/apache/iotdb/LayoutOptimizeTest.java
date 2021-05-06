package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LayoutOptimizeTest {
  private static Session session;
  private static final String HOST = "127.0.0.1";
  private static final String STORAGE_GROUP = "root.sgtest";
  private static final String DEVICE = "root.sgtest.d1";
  private static final int TIMESERIES_NUM = 100;
  private static final long TIME_NUM = 10000L;

  public static void main(String[] args) throws Exception {
    session = new Session(HOST, 6667, "root", "root");
    session.open(false);

    setUpEnvironment();

    clearEnvironment();
  }

  public static void setUpEnvironment()
      throws IoTDBConnectionException, StatementExecutionException {
    session.setStorageGroup(STORAGE_GROUP);
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    for (int i = 0; i < TIMESERIES_NUM; ++i) {
      session.createTimeseries(
          DEVICE + ".s" + i, TSDataType.DOUBLE, TSEncoding.DIFF, CompressionType.PLA);
      measurements.add("s" + i);
      types.add(TSDataType.DOUBLE);
    }
    Random r = new Random();
    for (long time = 0; time < TIME_NUM; ++time) {
      List<Object> values = new ArrayList<>();
      for (int i = 0; i < TIMESERIES_NUM; i++) {
        values.add(r.nextDouble());
      }
      session.insertRecord(DEVICE, time, measurements, types, values);
    }
  }

  public static void clearEnvironment()
      throws IoTDBConnectionException, StatementExecutionException {
    session.deleteStorageGroup(STORAGE_GROUP);
  }

  public static void verifyGroupByWithoutValueFilter() {}
}
