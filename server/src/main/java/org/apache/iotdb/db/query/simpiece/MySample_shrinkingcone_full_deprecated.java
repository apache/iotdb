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
import java.io.PrintWriter;
import java.util.List;

public class MySample_shrinkingcone_full_deprecated {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList = new int[] {320, 360, 400, 440, 480, 520, 560, 600, 640};

    double[][] epsilonArray = {
      {
        14.388325214385986,
        13.768231868743896,
        13.343345165252686,
        13.091249942779541,
        12.69767427444458,
        12.391706943511963,
        12.167190074920654,
        11.972727298736572,
        11.756273746490479
      },
      {0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.001, 0.0009999999999999999},
      {
        680.2196469306946,
        665.2842917442322,
        650.2610821723938,
        636.7945942878723,
        620.9016032218933,
        604.7394433021545,
        592.9867577552795,
        578.6471419334412,
        570.5106825828552
      },
      {
        11.259880542755127,
        10.37297010421753,
        9.402754306793213,
        8.867334842681885,
        8.384315967559814,
        7.915340900421143,
        7.39516019821167,
        6.892223834991455,
        6.5178914070129395
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

    for (int i = 0; i < epsilonArray.length; i++) { // 遍历行
      for (int j = 0; j < epsilonArray[i].length; j++) { // 遍历列
        System.out.print(epsilonArray[i][j] + ",");
      }
      System.out.println();
    }
  }

  //  public static double getSCParam(int nout, TimeSeries ts, double accuracy) throws IOException {
  //    double epsilon = 1;
  //    boolean directLess = false;
  //    boolean directMore = false;
  //    while (true) {
  //      List<Point> reducedPoints = ShrinkingCone.reducePoints(ts.data, epsilon);
  //      if (reducedPoints.size() > nout) {
  //        if (directMore) {
  //          break;
  //        }
  //        if (!directLess) {
  //          directLess = true;
  //        }
  //        epsilon *= 2;
  //      } else {
  //        if (directLess) {
  //          break;
  //        }
  //        if (!directMore) {
  //          directMore = true;
  //        }
  //        epsilon /= 2;
  //      }
  //    }
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
  //    return (left + right) / 2;
  //  }
}
