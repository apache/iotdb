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

// Sim-Piece code forked from https://github.com/xkitsios/Sim-Piece.git

package org.apache.iotdb.db.query.simpiece;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public class MySample_dwt_full2 {
  // After running this,
  // output sample csv and copy them into lts-exp/notebook/segmentResults/.
  // output epsilonArray_*.txt and copy them into lts-exp/tools/.

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    // do not change the order of datasets below, as the output is used in exp bash
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList =
        new int[] {
          320, 400, 480, 580, 720, 960, 1200, 1600, 2000, 2400, 3000, 3600, 4000, 4400, 5000
        };

    double[][] epsilonArray = {
      {
        98.84062576293945,
        83.28593826293945,
        71.22443580627441,
        59.164398193359375,
        48.10093879699707,
        37.6534366607666,
        31.448437690734863,
        25.480151176452637,
        22.1484375,
        19.969138145446777,
        17.644524574279785,
        16.02500057220459,
        15.221578598022461,
        14.512500762939453,
        13.687499046325684,
      },
      {
        0.01904773712158203, 0.01746368408203125, 0.01591014862060547, 0.015031814575195312,
        0.013813018798828125, 0.011875152587890625, 0.010687828063964844, 0.009125709533691406,
        0.007843017578125, 0.007071495056152344, 0.0059661865234375, 0.005126953125,
        0.0046844482421875, 0.004241943359375, 0.0037126541137695312,
      },
      {
        17456.15777873993, 13712.626691818237, 10876.021079063416, 9463.918528556824,
        7767.749307632446, 5635.975116729736, 4259.744117736816, 3277.170015335083,
        2655.061099052429, 2169.5194940567017, 1751.8307266235352, 1485.040364265442,
        1367.6178674697876, 1255.3692474365234, 1127.6510620117188,
      },
      {
        849.467981338501,
        703.4278907775879,
        556.0022611618042,
        456.47415828704834,
        345.5801601409912,
        241.60498523712158,
        184.83848667144775,
        128.0,
        97.61968803405762,
        77.68981170654297,
        58.74324989318848,
        47.012436866760254,
        41.456562995910645,
        36.50571346282959,
        31.0604829788208,
      }
    };

    //    double[][] epsilonArray = new double[datasetNameList.length][];
    //    for (int i = 0; i < datasetNameList.length; i++) {
    //      epsilonArray[i] = new double[noutList.length];
    //    }

    for (int y = 0; y < datasetNameList.length; y++) {
      String datasetName = datasetNameList[y];
      int start = 0;
      int end = 1000_0000;
      int N = end - start;
      boolean hasHeader = false;
      try (FileInputStream inputStream = new FileInputStream(fileDir + datasetName + ".csv")) {
        String delimiter = ",";
        TimeSeries ts =
            TimeSeriesReader.getMyTimeSeries(
                inputStream, delimiter, false, N, start, hasHeader, true);
        double[] values = new double[ts.length()];
        int k = 0;
        for (Point p : ts.data) {
          values[k++] = p.getValue();
        }

        for (int x = 0; x < noutList.length; x++) {
          int nout = noutList[x];

          //          double epsilon = MySample_dwt.getDWTParam(nout, values, 1e-6);
          //          epsilonArray[y][x] = epsilon;

          double epsilon = epsilonArray[y][x];

          List<Point> reducedPoints = DWT.reducePoints(values, epsilon);
          System.out.println(
              datasetName
                  + ": n="
                  + N
                  + ",m="
                  + nout
                  + ",epsilon="
                  + epsilon
                  + ",actual m="
                  + reducedPoints.size());
          try (PrintWriter writer =
              new PrintWriter(
                  new FileWriter(
                      datasetName
                          + "-"
                          + N
                          + "-"
                          + nout
                          + "-"
                          + reducedPoints.size()
                          + "-dwt.csv"))) {
            for (Point p : reducedPoints) {
              writer.println(p.getTimestamp() + "," + p.getValue());
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    // do not change name of the output file, as the output is used in exp bash
    try (FileWriter writer = new FileWriter("epsilonArray_dwt.txt")) {
      for (double[] row : epsilonArray) {
        for (double element : row) {
          writer.write(element + " ");
          System.out.print(element + ",");
        }
        writer.write("\n");
        System.out.println();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
