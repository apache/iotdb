/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.tools.watermark;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;

import org.apache.thrift.EncodingUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.ZoneId;

public class WatermarkDetector {

  public static void main(String[] args) throws IOException, LogicalOperatorException {
    if (args == null || args.length != 8) {
      throw new IOException(
          "Usage: ./detect-watermark.sh [filePath] [secretKey] "
              + "[watermarkBitString] [embed_row_cycle] [embed_lsb_num] [alpha] [columnIndex] "
              + "[dataType: int/float/double]");
    }
    String filePath = args[0]; // data file path
    String secretKey = args[1]; // watermark secret key
    String watermarkBitString = args[2]; // watermark bit string
    int embed_row_cycle = Integer.parseInt(args[3]); // watermark parameter
    int embed_lsb_num = Integer.parseInt(args[4]); // watermark parameter
    double alpha = Double.parseDouble(args[5]); // significance level of watermark detection
    int columnIndex = Integer.parseInt(args[6]); // specify a column of data to detect
    String dataType = args[7].toLowerCase();

    if (embed_row_cycle < 1 || embed_lsb_num < 1 || alpha < 0 || alpha > 1 || columnIndex < 1) {
      throw new IOException("Parameter out of range.");
    }

    if (!dataType.equals("int") && !dataType.equals("float") && !dataType.equals("double")) {
      throw new IOException("invalid parameter: supported data types are int/float/double");
    }

    isWatermarked(
        filePath,
        secretKey,
        watermarkBitString,
        embed_row_cycle,
        embed_lsb_num,
        alpha,
        columnIndex,
        dataType);
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static boolean isWatermarked(
      String filePath,
      String secretKey,
      String watermarkBitString,
      int embed_row_cycle,
      int embed_lsb_num,
      double alpha,
      int columnIndex,
      String dataType)
      throws LogicalOperatorException, IOException {
    System.out.println("-----Watermark detection begins-----");
    int[] trueNums = new int[watermarkBitString.length()]; // for majority vote
    int[] falseNums = new int[watermarkBitString.length()]; // for majority vote
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line = reader.readLine(); // skip header
      String[] items = line.split(",");
      if (columnIndex < 1 || columnIndex > items.length - 1) {
        throw new IOException("columnIndex is out of range.");
      }
      while ((line = reader.readLine()) != null) {
        items = line.split(",");
        long timestamp = parseTimestamp(items[0]);
        if (GroupedLSBWatermarkEncoder.hashMod(
                String.format("%s%d", secretKey, timestamp), embed_row_cycle)
            == 0) {
          String str = items[columnIndex];
          if (str.equals("null")) {
            continue;
          }

          int targetBitPosition =
              GroupedLSBWatermarkEncoder.hashMod(
                  String.format("%s%d%s", secretKey, timestamp, secretKey), embed_lsb_num);
          int groupId =
              GroupedLSBWatermarkEncoder.hashMod(
                  String.format("%d%s", timestamp, secretKey), watermarkBitString.length());

          boolean isTrue = true;
          switch (dataType) {
            case "int":
              isTrue =
                  EncodingUtils.testBit(Integer.parseInt(items[columnIndex]), targetBitPosition);
              break;
            case "float":
              int floatToIntBits = Float.floatToIntBits(Float.parseFloat(items[columnIndex]));
              isTrue = EncodingUtils.testBit(floatToIntBits, targetBitPosition);
              break;
            case "double":
              long doubleToLongBits =
                  Double.doubleToLongBits(Double.parseDouble(items[columnIndex]));
              isTrue = EncodingUtils.testBit(doubleToLongBits, targetBitPosition);
              break;
            default:
          }
          if (isTrue) {
            trueNums[groupId] += 1;
          } else {
            falseNums[groupId] += 1;
          }
        }
      }
    }

    int cnt = 0; // total counted number
    int hit_cnt = 0; // detected hit number
    for (int i = 0; i < watermarkBitString.length(); i++) {
      int res = trueNums[i] - falseNums[i]; // majority vote
      if (res > 0 && watermarkBitString.charAt(i) == '1') {
        hit_cnt += 1;
      } else if (res < 0 && watermarkBitString.charAt(i) == '0') {
        hit_cnt += 1;
      }
      if (res != 0) {
        cnt += 1;
      } // otherwise trueNums[i]==falseNums[i] then it is not counted
    }

    int b = calMin(cnt, alpha);
    System.out.println(
        String.format("total counted number = %d, detected hit number = %d", cnt, hit_cnt));
    System.out.println(
        String.format(
            "To reach the significant level %f, the hit number should be not smaller than: %d",
            alpha, b));
    boolean isWatermarked;
    if (hit_cnt >= b) {
      System.out.println("Therefore the detection result is: watermarked");
      isWatermarked = true;
    } else {
      System.out.println("Therefore the detection result is: not watermarked");
      isWatermarked = false;
    }
    System.out.println("-----Watermark detection finishes-----");
    return isWatermarked;
  }

  /** Parses timestamp from string type to long type */
  private static long parseTimestamp(String str) throws LogicalOperatorException {
    long timestamp;
    try {
      timestamp = Long.parseLong(str);
    } catch (NumberFormatException e) {
      try {
        ZoneId zoneId = ZoneId.systemDefault();
        timestamp = DatetimeUtils.convertDatetimeStrToLong(str, zoneId);
      } catch (LogicalOperatorException e1) {
        throw new LogicalOperatorException("The format of timestamp is not unexpected.");
      }
    }
    return timestamp;
  }

  /**
   * Finds the minimum b that meets the formula: (C(l,b)+C(l,b+1)+C(l,b+2)+...+C(l,l))/2^l < alpha
   *
   * @param l the total number
   * @param alpha significance level
   * @return the minimum b
   */
  private static int calMin(int l, double alpha) {
    int b = l;
    BigDecimal sum = new BigDecimal("1");

    BigDecimal thrs = BigDecimal.valueOf(alpha);
    for (int i = 0; i < l; i++) {
      thrs = thrs.multiply(new BigDecimal("2"));
    }

    while (sum.compareTo(thrs) < 0) { // sum < thrs
      b -= 1;
      sum = sum.add(Comb(l, b));
    }

    b++;
    if (b > l) {
      System.out.println(
          "The total counted number or the alpha is too small to find b "
              + "that meets the formula: (C(l,b)+C(l,b+1)+C(l,b+2)+...+C(l,l))/2^l < alpha");
    }
    return b;
  }

  /** Calculates combinatorial number C(n,m). */
  private static BigDecimal Comb(int n, int m) {
    BigDecimal res1 = new BigDecimal("1");
    for (int i = n; i > m; i--) {
      res1 = res1.multiply(new BigDecimal(i));
    }
    BigDecimal res2 = new BigDecimal("1");
    for (int i = 2; i <= n - m; i++) {
      res2 = res2.multiply(new BigDecimal(i));
    }
    return res1.divide(res2, 10, RoundingMode.HALF_EVEN);
  }
}
