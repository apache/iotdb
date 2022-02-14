package org.apache.iotdb.writeData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class ExtractRcvTimeData {

  // java D:\RcvTime\dump0.csv RcvTime.csv 0 1 0 1644144767000
  public static void main(String[] args) throws IOException {
    String inPath = args[0];
    String outPath = args[1];
    int timeIdx = Integer.parseInt(args[2]); // 0
    int valueIdx = Integer.parseInt(args[3]); // mf03: 4
    long startTime = Long.parseLong(args[4]);
    long endTime = Long.parseLong(args[5]);

    File f = new File(inPath);
    FileWriter fileWriter = new FileWriter(outPath);
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    PrintWriter printWriter = new PrintWriter(fileWriter);
    int cnt = 0;
    long lastTimestamp = -1;

    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      String timestampStr = split[timeIdx];
      long timestamp = getInstantWithPrecision(timestampStr, "ms");
      if (timestamp >= endTime) {
        break;
      }
      if (timestamp < startTime) {
        continue;
      }
      if (timestamp <= lastTimestamp) {
        System.out.println("out-of-order! " + timestamp + "last: " + lastTimestamp);
        continue;
      }
      long value = Long.parseLong(split[valueIdx]);
      printWriter.print(timestamp);
      printWriter.print(",");
      printWriter.print(value);
      printWriter.println();
      cnt++;
      lastTimestamp = timestamp;
    }
    reader.close();
    printWriter.close();
  }

  private static long getInstantWithPrecision(String str, String timestampPrecision)
      throws IOException {
    try {
      DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
      ZonedDateTime zonedDateTime = ZonedDateTime.parse(str, formatter);
      Instant instant = zonedDateTime.toInstant();
      if (timestampPrecision.equals("us")) {
        if (instant.getEpochSecond() < 0 && instant.getNano() > 0) {
          // adjustment can reduce the loss of the division
          long millis = Math.multiplyExact(instant.getEpochSecond() + 1, 1000_000);
          long adjustment = instant.getNano() / 1000 - 1L;
          return Math.addExact(millis, adjustment);
        } else {
          long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000);
          return Math.addExact(millis, instant.getNano() / 1000);
        }
      } else if (timestampPrecision.equals("ns")) {
        long millis = Math.multiplyExact(instant.getEpochSecond(), 1000_000_000L);
        return Math.addExact(millis, instant.getNano());
      }
      return instant.toEpochMilli();
    } catch (DateTimeParseException e) {
      throw new IOException(e.getMessage());
    }
  }
}
