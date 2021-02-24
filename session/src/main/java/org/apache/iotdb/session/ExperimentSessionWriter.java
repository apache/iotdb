package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.ChunkSizeOptimizationResult;
import org.apache.iotdb.service.rpc.thrift.MeasurementOrder;
import org.apache.iotdb.service.rpc.thrift.MeasurementOrderSet;
import org.apache.iotdb.service.rpc.thrift.ReplicaSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ExperimentSessionWriter {
  private static final Session session = new Session("192.168.130.38", 6667, "root", "root");
  private static final int TIMESERIES_NUM = 1000;
  private static int DATA_NUM = 10000;
  private static final File COST_LOG_FILE = new File("E:\\Thing\\Workspace\\IoTDB\\res\\Order_3R.cost");
  private static final File CHUNK_SIZE_OPT_LOG_FILE = new File("E:\\Thing\\Workspace\\IoTDB\\res\\ChunkSizeOpt.txt");
  private static OutputStream COST_LOG_STREAM;
  public static void main(String[] args) throws Exception{
    if (!COST_LOG_FILE.exists()) {
      COST_LOG_FILE.createNewFile();
    }
    COST_LOG_STREAM = new FileOutputStream(COST_LOG_FILE);

    session.open(false);
    session.readRecordFromFile();
    session.readMetadataFromFile();
    session.deleteStorageGroup("root.test");
    createTimeseries();
    testMultipleReplicaSA();
    session.close();
  }

  static void createTimeseries() throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < TIMESERIES_NUM; ++i) {
      System.out.println("Creating TimeSeries" + i);
      session.createTimeseries(
              "root.test.device.s" + String.valueOf(i),
              TSDataType.DOUBLE,
              TSEncoding.GORILLA,
              CompressionType.SNAPPY);
    }
  }

  static void generateData() throws Exception {
    Random r = new Random();
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int s = 0; s < TIMESERIES_NUM; ++s) {
      schemaList.add(new MeasurementSchema("s" + s, TSDataType.DOUBLE));
    }
    Tablet tablet = new Tablet("root.test.device", schemaList, 2000);
    long timestamp = 0;
    int rowIdx = 0;
    for (int j = 0; j < DATA_NUM*5; ++j) {
      rowIdx = tablet.rowSize++;
      timestamp++;
      tablet.addTimestamp(rowIdx, timestamp);
      for(int s = 0; s < TIMESERIES_NUM; ++s) {
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIdx, r.nextDouble());
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }
    if (tablet.rowSize != 0) {
      session.insertTablet(tablet, true);
    }
  }

  static void showOrderSet(MeasurementOrderSet orderSet) {
    List<MeasurementOrder> orders = orderSet.getMeasurementsOrders();
    for(MeasurementOrder order : orders) {
      for(String measurement : order.measurements) {
        System.out.print(measurement + " ");
      }
      System.out.println();
      System.out.println();
    }
  }

  static void showReplicaSet(ReplicaSet replicaSet) {
    for(int i = 0; i < replicaSet.measurementOrders.size(); ++i) {
      MeasurementOrder order = replicaSet.measurementOrders.get(i);
      for(String measurement : order.getMeasurements()) {
        System.out.print(measurement + " ");
      }
      System.out.println();
      List<String> workload = replicaSet.workloadPartition.get(i);
      for(String sql : workload) {
        System.out.println(sql);
      }
      System.out.println();
      System.out.println();
    }
  }

  static void writeCostLog(List<Double> costList) {
    StringBuilder builder = new StringBuilder();
    for(Double cost : costList) {
      builder.append(cost);
      builder.append('\n');
    }
    try {
      COST_LOG_STREAM.write(builder.toString().getBytes());
      COST_LOG_STREAM.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void testChunkSizeOptimze() {
    List<String> measurements = new ArrayList<>();
    List<String> ops = new ArrayList<>();
    measurements.add("s500");
    ops.add("avg");
    List<String> measurementOrders = new ArrayList<>();
    for(int i = 0; i < 1000; ++i) {
      measurementOrders.add("s" + i);
    }
    long startTime = 2200l;
    long endTime = 3400l;
    try {
      ChunkSizeOptimizationResult result = session.runOptimizeChunkSize(measurements, ops, startTime, endTime, measurementOrders);
      if (!CHUNK_SIZE_OPT_LOG_FILE.exists()) {
        CHUNK_SIZE_OPT_LOG_FILE.createNewFile();
      }
      OutputStream os = new FileOutputStream(CHUNK_SIZE_OPT_LOG_FILE);
      StringBuilder builder = new StringBuilder();
      for(int i = 0; i < result.chunkSize.size(); ++i) {
        builder.append(result.chunkSize.get(i));
        builder.append(' ');
        builder.append(result.cost.get(i));
        builder.append('\n');
      }
      os.write(builder.toString().getBytes());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static void testMultipleReplicaSA() {
    try {
      long startTime = System.currentTimeMillis();
      ReplicaSet replicaSet = session.runMultiReplicaOptimize("root.test.device");
      long lastTime = System.currentTimeMillis() - startTime;
      System.out.println(lastTime / 1000l + " s");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
