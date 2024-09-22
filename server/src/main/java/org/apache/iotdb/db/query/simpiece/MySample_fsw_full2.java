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

public class MySample_fsw_full2 {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList =
        new int[] {
          320, 400, 480, 580, 720, 960, 1200, 1600, 2000, 2400, 3000, 3600, 4000, 4400, 5000
        };

    double[][] epsilonArray = {
      {
        9.618271827697754,
        8.991525650024414,
        8.375,
        7.99461555480957,
        7.615596771240234,
        7.172431945800781,
        6.877520561218262,
        6.408135414123535,
        6.219999313354492,
        5.977571487426758,
        5.679183006286621,
        5.499468803405762,
        5.387459754943848,
        5.299999237060547,
        5.1635847091674805,
      },
      {
        9.9945068359375E-4, 9.984970092773438E-4, 9.965896606445312E-4, 9.937286376953125E-4,
        9.899139404296875E-4, 9.813308715820312E-4, 9.6893310546875E-4, 9.42230224609375E-4,
        8.983612060546875E-4, 8.268356323242188E-4, 6.685256958007812E-4, 5.197525024414062E-4,
        4.99725341796875E-4, 4.987716674804688E-4, 4.8732757568359375E-4,
      },
      {
        440.0235958099365, 423.5567502975464, 405.7711305618286, 396.31347465515137,
        380.6776990890503, 358.0119905471802, 336.5447692871094, 303.85207748413086,
        279.7541666030884, 257.3554916381836, 232.57367420196533, 211.35449981689453,
        200.9514446258545, 192.0128345489502, 178.28646087646484,
      },
      {
        9.295397758483887, 7.5366668701171875, 6.473535537719727, 5.6244354248046875,
        4.713288307189941, 3.7258691787719727, 3.078885078430176, 2.4637460708618164,
        2.0368423461914062, 1.7683038711547852, 1.4741735458374023, 1.266657829284668,
        1.1668891906738281, 1.071258544921875, 0.9608230590820312,
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
                inputStream, delimiter, false, N, start, hasHeader, false);
        for (int x = 0; x < noutList.length; x++) {
          int nout = noutList[x];

          //          double epsilon = getFSWParam(nout, ts, 1e-6);
          //          epsilonArray[y][x] = epsilon;

          double epsilon = epsilonArray[y][x];

          List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
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
                          + "-fsw.csv"))) {
            for (Point p : reducedPoints) {
              writer.println(p.getTimestamp() + "," + p.getValue());
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    for (int i = 0; i < epsilonArray.length; i++) {
      for (int j = 0; j < epsilonArray[i].length; j++) {
        System.out.print(epsilonArray[i][j] + ",");
      }
      System.out.println();
    }
  }

  public static double getFSWParam(int nout, TimeSeries ts, double accuracy) throws IOException {
    double epsilon = 1;
    boolean directLess = false;
    boolean directMore = false;
    boolean skip = false; // TODO
    int threshold = 2; // TODO
    while (true) {
      List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
      if (reducedPoints.size() > nout) {
        if (directMore) {
          // TODO
          if (Math.abs(reducedPoints.size() - nout) <= threshold) {
            skip = true;
          }
          //
          break;
        }
        if (!directLess) {
          directLess = true;
        }
        epsilon *= 2;
      } else {
        if (directLess) {
          // TODO
          if (Math.abs(nout - reducedPoints.size()) <= threshold) {
            skip = true;
          }
          //
          break;
        }
        if (!directMore) {
          directMore = true;
        }
        epsilon /= 2;
      }
    }
    // TODO
    if (skip) {
      return epsilon;
    }
    //
    // begin dichotomy
    double left = 0;
    double right = 0;
    if (directLess) {
      left = epsilon / 2;
      right = epsilon;
    }
    if (directMore) {
      left = epsilon;
      right = epsilon * 2;
    }
    while (Math.abs(right - left) > accuracy) {
      double mid = (left + right) / 2;
      List<Point> reducedPoints = FSW.reducePoints(ts.data, mid);
      if (reducedPoints.size() > nout) {
        left = mid;
      } else {
        right = mid;
      }
    }

    // TODO
    List<Point> reducedPoints = FSW.reducePoints(ts.data, left);
    int n1 = reducedPoints.size();
    reducedPoints = FSW.reducePoints(ts.data, right);
    int n2 = reducedPoints.size();
    if (Math.abs(n1 - nout) < Math.abs(n2 - nout)) {
      return left;
    } else {
      return right;
    }
    //
  }
}
