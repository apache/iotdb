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

public class WriteMF03 {
  /** Before writing data, make sure check the server parameter configurations. */
  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    String measurement = "mf03"; // [[update]]
    String device = "root.debs2012"; // [[update]]
    long chunkAvgTimeLen = 102213719111L; // ns [[update]]

    // 实验自变量1：乱序数据源
    String filePath = args[0];
    // 实验自变量2：delete percentage
    int deletePercentage = Integer.parseInt(args[1]); // 0 means no deletes. 0-100
    // 实验自变量3：每次删除的时间长度，用chunkAvgTimeLen的百分比表示
    int deleteLenPercentage = Integer.parseInt(args[2]); // 0-100
    // 参数4：时间戳idx，从0开始
    int timeIdx = Integer.parseInt(args[3]);
    // 参数5：值idx，从0开始
    int valueIdx = Integer.parseInt(args[4]);

    if (deletePercentage < 0 || deletePercentage > 100) {
      throw new IOException("WRONG deletePercentage!");
    }
    if (deleteLenPercentage < 0 || deleteLenPercentage > 100) {
      throw new IOException("WRONG deleteLenPercentage!");
    }

    int avgSeriesPointNumberThreshold = 1000; // fixed
    int deletePeriod =
        (int) Math.floor(100 * 1.0 / deletePercentage * avgSeriesPointNumberThreshold);
    long deleteLen = (long) Math.floor(chunkAvgTimeLen * deleteLenPercentage * 1.0 / 100);

    List<String> deletePaths = new ArrayList<>();
    deletePaths.add(device + "." + measurement);

    TSDataType tsDataType = TSDataType.INT64; // value types

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // this is to make all following inserts unseq chunks
    session.insertRecord(
        device,
        1644181628000000000L, // ns
        // NOTE UPDATE TIME DATATYPE! [[update]]. DONT USE System.nanoTime()!
        Collections.singletonList(measurement),
        Collections.singletonList(tsDataType), // NOTE UPDATE VALUE DATATYPE!
        0L); // NOTE UPDATE VALUE DATATYPE!
    session.executeNonQueryStatement("flush");

    File f = new File(filePath);
    String line = null;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    long lastDeleteMinTime = Long.MAX_VALUE;
    long lastDeleteMaxTime = Long.MIN_VALUE;
    int cnt = 0;
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.parseLong(split[timeIdx]);
      long value = Long.parseLong(split[valueIdx]);
      session.insertRecord(
          device,
          timestamp,
          Collections.singletonList(measurement),
          Collections.singletonList(tsDataType),
          value);
      cnt++;

      if (timestamp > lastDeleteMaxTime) {
        lastDeleteMaxTime = timestamp;
      }
      if (timestamp < lastDeleteMinTime) {
        lastDeleteMinTime = timestamp;
      }

      if (deletePercentage != 0) {
        if (cnt >= deletePeriod) {
          cnt = 0;
          // randomize deleteStartTime in [lastMinTime, max(lastMaxTime-deleteLen,lastMinTime+1)]
          long rightBound = Math.max(lastDeleteMaxTime - deleteLen, lastDeleteMinTime + 1);
          long deleteStartTime =
              (long)
                  Math.ceil(
                      lastDeleteMinTime + Math.random() * (rightBound - lastDeleteMinTime + 1));
          long deleteEndTime = deleteStartTime + deleteLen - 1;
          session.deleteData(deletePaths, deleteStartTime, deleteEndTime);
          System.out.println("[[[[delete]]]]]" + deleteStartTime + "," + deleteEndTime);

          lastDeleteMinTime = Long.MAX_VALUE;
          lastDeleteMaxTime = Long.MIN_VALUE;
        }
      }
    }

    session.executeNonQueryStatement("flush");
    session.close();
  }
}
