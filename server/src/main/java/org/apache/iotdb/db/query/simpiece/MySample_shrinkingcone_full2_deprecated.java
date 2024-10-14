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

public class MySample_shrinkingcone_full2_deprecated {
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
        14.388325263280421, 13.34334554336965, 12.697674418624956, 12.053359143726993,
        11.411823799891863, 10.700932090578135, 10.222302158304956, 9.64046321529895,
        9.169320066343062, 8.824726661085151, 8.38911917101359, 8.050000000046566,
        7.844965517288074, 7.675000000046566, 7.4411764705437236,
      },
      {
        0.001,
        0.001,
        0.001,
        0.001,
        0.001,
        0.001,
        0.001,
        9.999999892897904E-4,
        9.999999892897904E-4,
        9.999999892897904E-4,
        9.986772784031928E-4,
        9.954648558050394E-4,
        9.931506938301027E-4,
        9.859154815785587E-4,
        9.090908570215106E-4,
      },
      {
        680.4174693996902, 650.2610825433512, 621.0945158457616, 586.9638199705514,
        547.4329924675985, 494.63679197325837, 454.7579276354518, 406.4603661468718,
        368.9741049989243, 342.1729954186012, 305.74686436593765, 276.5570874213008,
        258.8369336259784, 243.3460398996831, 224.25107966241194,
      },
      {
        11.258956845791545, 9.40259607497137, 8.384315740317106, 7.238058144459501,
        5.828739010321442, 4.60251481551677, 3.8172692629741505, 3.0447826087474823,
        2.523470887565054, 2.170023310056422, 1.8160594796063378, 1.556580246950034,
        1.4294884910923429, 1.3174383605946787, 1.1894954767194577,
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
        for (int x = 0; x < noutList.length; x++) {
          int nout = noutList[x];

          //          double epsilon = MySample_shrinkingcone.getSCParam(nout, ts, 1e-10);
          //          epsilonArray[y][x] = epsilon;

          double epsilon = epsilonArray[y][x];

          List<Point> reducedPoints = ShrinkingCone.reducePoints(ts.data, epsilon);
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
                          + "-sc.csv"))) {
            for (Point p : reducedPoints) {
              writer.println(p.getTimestamp() + "," + p.getValue());
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    //    for (int i = 0; i < epsilonArray.length; i++) { // 遍历行
    //      for (int j = 0; j < epsilonArray[i].length; j++) { // 遍历列
    //        System.out.print(epsilonArray[i][j] + ",");
    //      }
    //      System.out.println();
    //    }

    // do not change name of the output file, as the output is used in exp bash
    try (FileWriter writer = new FileWriter("epsilonArray_sc.txt")) {
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

  //  public static double getSCParam(int nout, TimeSeries ts, double accuracy) throws IOException {
  //    double epsilon = 1;
  //    boolean directLess = false;
  //    boolean directMore = false;
  //    boolean skip = false; // TODO
  //    int threshold = 2; // TODO
  //    while (true) {
  //      List<Point> reducedPoints = ShrinkingCone.reducePoints(ts.data, epsilon);
  //      if (reducedPoints.size() > nout) {
  //        if (directMore) {
  //          // TODO
  //          if (Math.abs(reducedPoints.size() - nout) <= threshold) {
  //            skip = true;
  //          }
  //          //
  //          break;
  //        }
  //        if (!directLess) {
  //          directLess = true;
  //        }
  //        epsilon *= 2;
  //      } else {
  //        if (directLess) {
  //          // TODO
  //          if (Math.abs(nout - reducedPoints.size()) <= threshold) {
  //            skip = true;
  //          }
  //          //
  //          break;
  //        }
  //        if (!directMore) {
  //          directMore = true;
  //        }
  //        epsilon /= 2;
  //      }
  //    }
  //    // TODO
  //    if (skip) {
  //      return epsilon;
  //    }
  //    //
  //    // begin dichotomy
  //    double left = 0;
  //    double right = 0;
  //    if (directLess) {
  //      left = epsilon / 2;
  //      right = epsilon;
  //    }
  //    if (directMore) {
  //      left = epsilon;
  //      right = epsilon * 2;
  //    }
  //    while (Math.abs(right - left) > accuracy) {
  //      double mid = (left + right) / 2;
  //      List<Point> reducedPoints = ShrinkingCone.reducePoints(ts.data, mid);
  //      if (reducedPoints.size() > nout) {
  //        left = mid;
  //      } else {
  //        right = mid;
  //      }
  //    }
  //    // TODO
  //    List<Point> reducedPoints = FSW.reducePoints(ts.data, left);
  //    int n1 = reducedPoints.size();
  //    reducedPoints = FSW.reducePoints(ts.data, right);
  //    int n2 = reducedPoints.size();
  //    if (Math.abs(n1 - nout) < Math.abs(n2 - nout)) {
  //      return left;
  //    } else {
  //      return right;
  //    }
  //    //
  //  }
}
