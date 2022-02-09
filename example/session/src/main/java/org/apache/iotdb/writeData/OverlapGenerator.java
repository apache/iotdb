package org.apache.iotdb.writeData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 每次快要写到10000个点（chunk size）还剩下OD%个点的时候，OP%的概率决定要不要乱序. 如果要乱序，
 * 方法是剩下的这OD%个点，和接下来再OD%个点，合在一起2*OD%个点随机打乱然后再写。 0<=OP<=100 0<OD<=50.
 */
public class OverlapGenerator {

  public static void main(String[] args) throws IOException {
    // default
    int avgSeriesPointNumberThreshold = 1000;

    String inPath = args[0];
    String outPath = args[1];
    int timeIdx = Integer.parseInt(args[2]);
    int valueIdx = Integer.parseInt(args[3]);
    int overlapPercentage = Integer.parseInt(args[4]); // 0-100
    int overlapDepth = Integer.parseInt(args[5]); // 0-50

    if (overlapPercentage < 0 || overlapPercentage > 100) {
      throw new IOException("WRONG overlapPercentage");
    }
    if (overlapDepth > 50 || overlapDepth < 0) { // 暂时只实现最多相邻两个chunk重叠，重叠深度从0到50%
      throw new IOException("WRONG pointNum");
    }
    int pointNum = (int) Math.floor(avgSeriesPointNumberThreshold * (overlapDepth / 100.0));

    File f = new File(inPath);
    FileWriter fileWriter = new FileWriter(outPath);
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    PrintWriter printWriter = new PrintWriter(fileWriter);
    int cnt = 0;
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.parseLong(split[timeIdx]); // time
      long value = Long.parseLong(split[valueIdx]); // value

      // note the first timestamp is never disordered. is global minimal.
      printWriter.print(timestamp);
      printWriter.print(",");
      printWriter.print(value);
      printWriter.println();
      cnt++;

      if (overlapPercentage != 0) {
        while (cnt % avgSeriesPointNumberThreshold == avgSeriesPointNumberThreshold - pointNum) {
          if (new Random().nextDouble() <= overlapPercentage / 100.0) {
            // disturb the next 2*pointNum points
            long[] timestampArray = new long[2 * pointNum];
            long[] valueArray = new long[2 * pointNum];
            List<Integer> idx = new ArrayList<>();
            int n = 0;
            while (n < 2 * pointNum && (line = reader.readLine()) != null) {
              // don't change the sequence of the above two conditions
              split = line.split(",");
              timestampArray[n] = Long.parseLong(split[timeIdx]);
              valueArray[n] = Long.parseLong(split[valueIdx]);
              idx.add(n);
              n++;
            }
            Collections.shuffle(idx);
            for (Integer integer : idx) {
              int k = integer;
              printWriter.print(timestampArray[k]);
              printWriter.print(",");
              printWriter.print(valueArray[k]);
              printWriter.println();
              cnt++;
            }
          } else {
            int n = 0;
            while (n < 2 * pointNum && (line = reader.readLine()) != null) {
              // don't change the sequence of the above two conditions
              split = line.split(",");
              timestamp = Long.parseLong(split[timeIdx]); // time
              value = Long.parseLong(split[valueIdx]); // value
              printWriter.print(timestamp);
              printWriter.print(",");
              printWriter.print(value);
              printWriter.println();
              cnt++;
              n++;
            }
          }
        }
      }
    }
    System.out.println(cnt);
    reader.close();
    printWriter.close();
  }
}
