package org.apache.iotdb.writeData;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class WriteFullGame_old {

  /** Before writing data, make sure check the server parameter configurations. */
  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String measurements = "s6";
    String device = "root.game";

    List<String> deletePaths = new ArrayList<>();
    deletePaths.add(device + "." + measurements);

    // 实验自变量1：乱序数据源
    String filePath = args[0];
    // 实验自变量2：每隔deleteFreq时间（本实验时间单位ms）就执行一次删除
    long deleteFreq = Long.parseLong(args[1]); // 0 means no deletes
    // 实验自变量3：每次删除的时间长度
    long deleteLen = Long.parseLong(args[2]);
    // 参数4：时间戳idx，从0开始
    int timeIdx = Integer.parseInt(args[3]);
    // 参数5：值idx，从0开始
    int valueIdx = Integer.parseInt(args[4]);

    TSDataType tsDataType = TSDataType.INT64;

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // this is to make all following inserts unseq chunks
    session.insertRecord(
        device,
        System.nanoTime(), // NOTE UPDATE TIME DATATYPE!
        Collections.singletonList(measurements),
        Collections.singletonList(tsDataType), // NOTE UPDATE VALUE DATATYPE!
        0L); // NOTE UPDATE VALUE DATATYPE!!!
    session.executeNonQueryStatement("flush");

    long minTime = -1;
    File f = new File(filePath);
    String line = null;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    int lastDeleteIdx = -1;
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.parseLong(split[timeIdx]);
      if (minTime == -1) {
        minTime = timestamp; // assume first timestamp is never disordered. is global minimal.
        timestamp = 0;
      } else {
        timestamp = timestamp - minTime;
      }
      timestamp = (long) (timestamp / 1000); // turn to ns. original time unit is ps. IoTDB only ns.

      if (deleteFreq != 0) {
        int idx = (int) Math.floor(timestamp * 1.0 / deleteFreq);
        //        System.out.println(timestamp + "," + deleteFreq + "," + idx);
        if (idx > 0 && idx != lastDeleteIdx) {
          lastDeleteIdx = idx;
          // [timestamp-deleteFreq, timestamp-1]内随机取一个删除时间起点
          long min = timestamp - deleteFreq;
          long max = timestamp - 1;
          long deleteStartTime = min + (((long) (new Random().nextDouble() * (max - min))));
          long deleteEndTime = deleteStartTime + deleteLen - 1;
          session.deleteData(deletePaths, deleteStartTime, deleteEndTime);

          //          System.out.println("[[[[delete]]]]]" + deleteStartTime + "," + deleteEndTime);

        }
      }

      long value = Long.parseLong(split[valueIdx]);
      session.insertRecord(
          device,
          timestamp,
          Collections.singletonList(measurements),
          Collections.singletonList(tsDataType),
          value);
    }

    session.executeNonQueryStatement("flush");
    session.close();
  }
}
