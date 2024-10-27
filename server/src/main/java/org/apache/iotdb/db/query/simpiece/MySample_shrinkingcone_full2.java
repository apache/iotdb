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

public class MySample_shrinkingcone_full2 {
  // After running this,
  // output sample csv and copy them into lts-exp/notebook/segmentResults/.
  // output epsilonArray_*.txt and copy them into lts-exp/tools/.

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    // do not change the order of datasets below, as the output is used in exp bash
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList = new int[] {320, 480, 740, 1200, 2000, 3500, 6000, 10000, 15000};

    double[][] epsilonArray = {
      {
        28.350000381469727,
        24.75,
        20.15000057220459,
        7.495454788208008,
        5.978656768798828,
        5.123478889465332,
        4.4909868240356445,
        3.9743595123291016,
        3.59999942779541,
      },
      {
        0.001,
        9.9945068359375E-4,
        9.9945068359375E-4,
        9.9945068359375E-4,
        9.9945068359375E-4,
        9.984970092773438E-4,
        9.517669677734375E-4,
        0.0,
        0.0,
      },
      {
        868.2230234146118, 824.095006942749, 772.8191223144531, 709.9235706329346,
        632.2311534881592, 527.634295463562, 421.8133182525635, 322.99202156066895,
        247.5426368713379,
      },
      {
        17.567204475402832, 13.436342239379883, 10.276226997375488, 7.288450241088867,
        4.86691951751709, 2.9565000534057617, 1.8069705963134766, 1.1284055709838867,
        0.8002557754516602,
      }
    };

    //    double[][] epsilonArray = new double[datasetNameList.length][];
    //    for (int i = 0; i < datasetNameList.length; i++) {
    //      epsilonArray[i] = new double[noutList.length];
    //    }

    for (int y = 0; y < datasetNameList.length; y++) {
      //      if (y != 1) {
      //        continue;
      //      }

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
          //          if (y == 1 && x > 6) {
          //            continue; // because SC on Qloss special?
          //          }

          int nout = noutList[x];
          //
          //          double epsilon = MySample_shrinkingcone.getSCParam(nout, ts, 1e-6);
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
