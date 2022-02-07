// package org.apache.iotdb.writeData;
//
// import org.apache.iotdb.rpc.IoTDBConnectionException;
// import org.apache.iotdb.rpc.StatementExecutionException;
// import org.apache.iotdb.session.Session;
// import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
//
// import java.io.BufferedReader;
// import java.io.File;
// import java.io.FileReader;
// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.List;
// import java.util.Random;
//
// public class WriteFullGameDisorderedWithDeletes {
//
//  /** Before writing data, make sure check the server parameter configurations. */
//  public static void main(String[] args)
//      throws IoTDBConnectionException, StatementExecutionException, IOException {
//    String measurements = "s6";
//    String device = "root.game";
//    List<String> deletePaths = new ArrayList<>();
//    deletePaths.add(device + "." + measurements);
//
//    // 实验自变量1：乱序数据源
//    String filePath = args[0];
//    // 实验自变量2：每隔deleteFreq时间（本实验时间单位ms）就执行一次删除
//    long deleteFreq = Long.parseLong(args[1]); // 0 means no deletes
//    // 实验自变量3：每次删除的时间长度
//    long deleteLen = Long.parseLong(args[2]);
//
//    Session session = new Session("127.0.0.1", 6667, "root", "root");
//    session.open(false);
//
//    long minTime = -1;
//    File f = new File(filePath);
//    String line = null;
//    BufferedReader reader = new BufferedReader(new FileReader(f));
//    while ((line = reader.readLine()) != null) {
//      String[] split = line.split(",");
//      long timestamp = Long.valueOf(split[3]);
//      if (minTime == -1) {
//        minTime = timestamp;
//        timestamp = 0;
//      } else {
//        timestamp = timestamp - minTime;
//      }
//      timestamp = (long) (timestamp / 1000); // turn to ns. original time unit is ps. IoTDB only
// ns.
//
//      if (deleteFreq != 0 && timestamp != 0 && timestamp % deleteFreq == 0) {
//        // [timestamp-deleteFreq, timestamp-1]内随机取一个删除时间起点
//        long min = timestamp - deleteFreq;
//        long max = timestamp - 1;
//        long deleteStartTime = min + (((long) (new Random().nextDouble() * (max - min))));
//        long deleteEndTime = deleteStartTime + deleteLen - 1;
//        session.deleteData(deletePaths, deleteStartTime, deleteEndTime);
//      }
//
//      long value = Long.valueOf(split[7]);
//      session.insertRecord(
//          device,
//          timestamp,
//          Collections.singletonList(measurements),
//          Collections.singletonList(TSDataType.INT64),
//          value);
//    }
//
//    session.executeNonQueryStatement("flush");
//    session.close();
//  }
// }
