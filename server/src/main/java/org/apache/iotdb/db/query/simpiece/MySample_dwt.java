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

public class MySample_dwt {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList = new String[] {"Qloss", "Pyra1", "RTD", "WindSpeed"};
    int[] noutList = new int[] {100};
    double[] r = new double[] {0.1, 0.5, 1.3, 0};
    int[] NList = new int[] {2500000, 2500000, 2500000, 500000};
    double[] epsilonList =
        new double[] {
          0.01922464370727539, 15034.107696056366, 552.0329060554504, 67.42263174057007
        };
    //    double[] epsilonList =
    //        new
    // double[]{0.019301805417701563,15411.426927099063,552.0329062500005,67.35937500000003};
    for (int y = 0; y < datasetNameList.length; y++) {
      String datasetName = datasetNameList[y];
      int start = (int) (10000000 / 2 - NList[y] * r[y]); // 从0开始计数
      int end = (int) (10000000 / 2 + NList[y] * (1 - r[y]));
      int N = end - start;

      for (int nout : noutList) {
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
          double epsilon = getDWTParam(nout, values, 1e-6);
          //          double epsilon = epsilonList[y];
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
                      datasetName + "-" + N + "-" + reducedPoints.size() + "-dwt.csv"))) {
            for (Point p : reducedPoints) {
              writer.println(p.getTimestamp() + "," + p.getValue());
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static double getDWTParam(int nout, double[] values, double accuracy) throws IOException {
    double epsilon = 1;
    boolean directLess = false;
    boolean directMore = false;
    boolean skip = false;
    int threshold = 2;
    while (true) {
      List<Point> reducedPoints = DWT.reducePoints(values, epsilon);
      if (reducedPoints.size() > nout) {
        if (directMore) {
          if (Math.abs(reducedPoints.size() - nout) <= threshold) {
            skip = true;
          }
          break;
        }
        if (!directLess) {
          directLess = true;
        }
        epsilon *= 2;
      } else {
        if (directLess) {
          if (Math.abs(nout - reducedPoints.size()) <= threshold) {
            skip = true;
          }
          break;
        }
        if (!directMore) {
          directMore = true;
        }
        epsilon /= 2;
      }
    }
    if (skip) {
      return epsilon;
    }

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
      List<Point> reducedPoints = DWT.reducePoints(values, mid);
      if (reducedPoints.size() > nout) {
        left = mid;
      } else {
        right = mid;
      }
    }

    List<Point> reducedPoints = DWT.reducePoints(values, left);
    int n1 = reducedPoints.size();
    reducedPoints = DWT.reducePoints(values, right);
    int n2 = reducedPoints.size();
    if (Math.abs(n1 - nout) < Math.abs(n2 - nout)) {
      return left;
    } else {
      return right;
    }
  }
}
