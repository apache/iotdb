package org.apache.iotdb.writeData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class TestFullGameTmp {

  public static void main(String[] args) throws IOException {
    String path = args[0];
    File f = new File(path);
    String line = null;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    long lastTimestamp = 0L;
    int cnt = 0;
    int totalcnt = 0;
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.valueOf(split[3]);
      totalcnt++;
      if (timestamp < lastTimestamp) {
        //        System.out.println("disordered!");
        cnt++;
      }
      lastTimestamp = timestamp;
    }
    System.out.println("total disorder points=" + cnt);
    System.out.println("total points=" + totalcnt);
    System.out.println(cnt / totalcnt);
  }
}
