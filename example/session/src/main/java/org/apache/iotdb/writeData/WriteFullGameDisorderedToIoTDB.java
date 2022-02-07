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
// import java.util.Collections;
//
// public class WriteFullGameDisorderedToIoTDB {
//
//  public static void main(String[] args)
//      throws IOException, IoTDBConnectionException, StatementExecutionException {
//
//    String path = args[0];
//
//    String measurements = "s6";
//    String device = "root.game";
//
//    Session session = new Session("127.0.0.1", 6667, "root", "root");
//    session.open(false);
//
//    long minTime = -1;
//    File f = new File(path);
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
//      timestamp = (long) (timestamp / 1000); // turn to ns
//      long value = Long.valueOf(split[7]);
//      session.insertRecord(
//          device,
//          timestamp,
//          Collections.singletonList(measurements),
//          Collections.singletonList(TSDataType.INT64),
//          value);
//    }
//    session.executeNonQueryStatement("flush");
//    session.close();
//  }
// }
