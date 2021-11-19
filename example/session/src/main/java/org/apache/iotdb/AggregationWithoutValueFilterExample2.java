package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class AggregationWithoutValueFilterExample2 {

  private static Session session; 
  private static String[] creationSqls =
          new String[] {
//                  "SET STORAGE GROUP TO root.vehicle.d0",
                  "CREATE ALIGNED TIMESERIES root.vehicle.d0(" +
                          "s0 INT32 encoding=RLE compressor=SNAPPY, "+
                          "s1 INT64 encoding=RLE compressor=SNAPPY, "+
                          "s2 FLOAT encoding=RLE compressor=SNAPPY, "+
                          "s3 TEXT encoding=PLAIN compressor=SNAPPY, "+
                          "s4 BOOLEAN encoding=PLAIN compressor=SNAPPY)"
          };

  private static String insertTemplate =
          "INSERT INTO root.vehicle.d0(timestamp,s0,s1,s2,s3,s4)" + " VALUES(%d,%d,%d,%f,%s,%s)";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);
    session.setQueryTimeout(9999999999L);
//    createAlignedTimeseries();
    query();

    session.close();
  }
  
  private static void createAlignedTimeseries()
      throws StatementExecutionException, IoTDBConnectionException {
    for(String sql:creationSqls){
      session.executeNonQueryStatement(sql);
    }

    List<String> multiMeasurementComponents = new ArrayList<>();
    for (int i = 0; i <= 4; i++) {
      multiMeasurementComponents.add("s" + i);
    }
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.INT64);
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.TEXT);
    dataTypes.add(TSDataType.BOOLEAN);
    // prepare BufferWrite file
    for (int i = 5000; i < 7000; i++) {
//      List<Object> values = new ArrayList<>();
//      values.add(i);
//      values.add((long)i);
//      values.add((float)i);
//      values.add("'" + i + "'");
//      values.add(true);
//      session.insertAlignedRecord("root.vehicle.d0", i, multiMeasurementComponents, dataTypes, values);
      session.executeNonQueryStatement(
              String.format(
                      Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
    }
    session.executeNonQueryStatement("FLUSH");
    for (int i = 7500; i < 8500; i++) {
//      List<Object> values = new ArrayList<>();
//      values.add(i);
//      values.add((long)i);
//      values.add((float)i);
//      values.add("'" + i + "'");
//      values.add(true);
//      session.insertAlignedRecord("root.vehicle.d0", i, multiMeasurementComponents, dataTypes, values);
      session.executeNonQueryStatement(
              String.format(
                      Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
    }
    session.executeNonQueryStatement("FLUSH");
    // prepare Unseq-File
    for (int i = 500; i < 1500; i++) {
//      List<Object> values = new ArrayList<>();
//      values.add(i);
//      values.add((long)i);
//      values.add((float)i);
//      values.add("'" + i + "'");
//      values.add(true);
//      session.insertAlignedRecord("root.vehicle.d0", i, multiMeasurementComponents, dataTypes, values);
      session.executeNonQueryStatement(
              String.format(
                      Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
    }
    session.executeNonQueryStatement("FLUSH");
    for (int i = 3000; i < 6500; i++) {
//      List<Object> values = new ArrayList<>();
//      values.add(i);
//      values.add((long)i);
//      values.add((float)i);
//      values.add("'" + i + "'");
//      values.add(true);
//      session.insertAlignedRecord("root.vehicle.d0", i, multiMeasurementComponents, dataTypes, values);
      session.executeNonQueryStatement(
              String.format(
                      Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
    }
    session.executeNonQueryStatement("merge");

    // prepare BufferWrite cache
    for (int i = 9000; i < 10000; i++) {
//      List<Object> values = new ArrayList<>();
//      values.add(i);
//      values.add((long)i);
//      values.add((float)i);
//      values.add("'" + i + "'");
//      values.add(true);
//      session.insertAlignedRecord("root.vehicle.d0", i, multiMeasurementComponents, dataTypes, values);
      session.executeNonQueryStatement(
              String.format(
                      Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "true"));
    }
    // prepare Overflow cache
    for (int i = 2000; i < 2500; i++) {
//      List<Object> values = new ArrayList<>();
//      values.add(i);
//      values.add((long)i);
//      values.add((float)i);
//      values.add("'" + i + "'");
//      values.add(true);
//      session.insertAlignedRecord("root.vehicle.d0", i, multiMeasurementComponents, dataTypes, values);
      session.executeNonQueryStatement(
              String.format(
                      Locale.ENGLISH, insertTemplate, i, i, i, (double) i, "'" + i + "'", "false"));
    }
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet =
        session.executeQueryStatement("select count(s1), sum(s2) from root.vehicle.d0 where time>10");
//        session.executeQueryStatement("select s1 from root.vehicle.d0");
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    while (dataSet.hasNext()) {
      System.out.println(dataSet.next());
    }
    dataSet.closeOperationHandle();
  }
}