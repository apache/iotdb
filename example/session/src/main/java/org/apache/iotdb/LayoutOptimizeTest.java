package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.*;
import java.util.*;

public class LayoutOptimizeTest {
  private static Session session;
  private static final String HOST = "127.0.0.1";
  private static final String STORAGE_GROUP = "root.sgtest";
  private static final String DEVICE = "root.sgtest.d1";
  private static final String OBJECT_FILE = "test.obj";
  private static final int TIMESERIES_NUM = 100;
  private static final long TIME_NUM = 10000L;

  public static void main(String[] args) throws Exception {
    session = new Session(HOST, 6667, "root", "root");
    session.open(false);
    //    clearEnvironment();
    //    setUpEnvironment();
    //    session.executeNonQueryStatement("flush");
    verifyAggregation();
    session.close();
  }

  public static void setUpEnvironment()
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.setStorageGroup(STORAGE_GROUP);
    } catch (StatementExecutionException e) {
    }
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    for (int i = 0; i < TIMESERIES_NUM; ++i) {
      session.createTimeseries(
          DEVICE + ".s" + i, TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
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

  public static void getResultOfGroupByWithoutValueFilter()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String sql = String.format("select avg(*) from %s group by ([0,%d),100ms)", DEVICE, TIME_NUM);
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    List<String> columnNames = dataSet.getColumnNames();
    Map<String, List<Double>> resultMap = new HashMap<>();
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (int i = 0; i < columnNames.size() - 1; i++) {
        String columnName = columnNames.get(i + 1);
        if (!resultMap.containsKey(columnName)) resultMap.put(columnName, new ArrayList<>());
        resultMap.get(columnName).add(fields.get(i).getDoubleV());
      }
    }
    File file = new File(OBJECT_FILE);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    ObjectOutputStream objectOutputStream =
        new ObjectOutputStream(new FileOutputStream(OBJECT_FILE));
    objectOutputStream.writeObject(resultMap);
    objectOutputStream.close();
  }

  public static void verifyGroupByWithoutValueFilter() throws Exception {
    String sql = String.format("select avg(*) from %s group by ([0,%d),100ms)", DEVICE, TIME_NUM);
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    List<String> columnNames = dataSet.getColumnNames();
    Map<String, List<Double>> resultMap = new HashMap<>();
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (int i = 0; i < columnNames.size() - 1; i++) {
        String columnName = columnNames.get(i + 1);
        if (!resultMap.containsKey(columnName)) resultMap.put(columnName, new ArrayList<>());
        resultMap.get(columnName).add(fields.get(i).getDoubleV());
      }
    }
    ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(OBJECT_FILE));
    Map<String, List<Double>> previousMap =
        (Map<String, List<Double>>) objectInputStream.readObject();
    System.out.println(resultMap.equals(previousMap));
  }

  public static void getResultOfAggregation() throws Exception {
    String sql = String.format("select avg(*) from %s", DEVICE);
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    Map<String, Double> resultMap = new HashMap<>();
    List<String> columns = dataSet.getColumnNames();
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (int i = 0; i < columns.size() - 1; ++i) {
        resultMap.put(columns.get(i), fields.get(i).getDoubleV());
      }
    }
    System.out.println(resultMap);
    File file = new File(OBJECT_FILE);
    if (file.exists()) {
      file.delete();
    }
    file.createNewFile();
    ObjectOutputStream objectOutputStream =
        new ObjectOutputStream(new FileOutputStream(OBJECT_FILE));
    objectOutputStream.writeObject(resultMap);
    objectOutputStream.close();
  }

  public static void verifyAggregation() throws Exception {
    String sql = String.format("select avg(*) from %s", DEVICE);
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    Map<String, Double> resultMap = new HashMap<>();
    List<String> columns = dataSet.getColumnNames();
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      for (int i = 0; i < columns.size() - 1; ++i) {
        resultMap.put(columns.get(i), fields.get(i).getDoubleV());
      }
    }
    ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(OBJECT_FILE));
    Map<String, Double> previousResult = (Map<String, Double>) objectInputStream.readObject();
    System.out.println(resultMap.equals(previousResult));
  }
}
