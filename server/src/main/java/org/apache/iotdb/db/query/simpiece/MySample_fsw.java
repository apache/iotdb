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

public class MySample_fsw {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList = new String[] {"Qloss", "Pyra1", "RTD", "WindSpeed"};
    int[] noutList = new int[] {100};
    double[] r = new double[] {0.1, 0.5, 1.3, 0};
    int[] NList = new int[] {2500000, 2500000, 2500000, 500000};
    double[] epsilonList =
        new double[] {
          9.999999992942321E-4, 284.40344031846143, 6.428162015438829, 10.818711800659003
        };
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
          //          double epsilon = getFSWParam(nout, ts, 1e-12);
          double epsilon = epsilonList[y];
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
                      datasetName + "-" + N + "-" + reducedPoints.size() + "-fsw.csv"))) {
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

  //  public static double getFSWParam(int nout, TimeSeries ts, double accuracy) throws IOException
  // {
  //    double epsilon = 1;
  //    boolean directLess = false;
  //    boolean directMore = false;
  //    while (true) {
  //      List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
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
  //      List<Point> reducedPoints = FSW.reducePoints(ts.data, mid);
  //      if (reducedPoints.size() > nout) {
  //        left = mid;
  //      } else {
  //        right = mid;
  //      }
  //    }
  //    return (left + right) / 2;
  //  }

  public static double getFSWParam(int nout, TimeSeries ts, double accuracy) throws IOException {
    double epsilon = 1;
    boolean directLess = false;
    boolean directMore = false;
    boolean skip = false;
    int threshold = 2;
    while (true) {
      List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
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
      List<Point> reducedPoints = FSW.reducePoints(ts.data, mid);
      if (reducedPoints.size() > nout) {
        left = mid;
      } else {
        right = mid;
      }
    }

    List<Point> reducedPoints = FSW.reducePoints(ts.data, left);
    int n1 = reducedPoints.size();
    reducedPoints = FSW.reducePoints(ts.data, right);
    int n2 = reducedPoints.size();
    if (Math.abs(n1 - nout) < Math.abs(n2 - nout)) {
      return left;
    } else {
      return right;
    }
  }
}
