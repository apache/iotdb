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
  // After running this,
  // output sample csv and copy them into lts-exp/notebook/segmentResults/.
  // output epsilonArray_*.txt and copy them into lts-exp/tools/.

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    // DO NOT change the order of datasets below, as the output is used in exp bash!!!
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList = new int[] {320, 480, 740, 1200, 2000, 3500, 6000, 10000, 15000};

    double[][] epsilonArray = {
      {
        18.988672256469727, 17.742149353027344, 15.250774383544922, 5.240921974182129,
        4.0604248046875, 3.5127735137939453, 3.09999942779541, 2.782364845275879,
        2.5470895767211914,
      },
      {
        9.9945068359375E-4,
        9.984970092773438E-4,
        9.946823120117188E-4,
        9.822845458984375E-4,
        9.49859619140625E-4,
        8.344650268554688E-4,
        4.949569702148438E-4,
        0.0,
        0.0,
      },
      {
        566.705379486084, 526.4058027267456, 489.42593574523926, 454.6960153579712,
        420.31038188934326, 375.1610279083252, 320.00675773620605, 254.37902355194092,
        196.95625495910645,
      },
      {
        14.356999397277832, 10.442278861999512, 8.121441841125488, 5.849754333496094,
        3.919279098510742, 2.444706916809082, 1.4873542785644531, 0.9297647476196289,
        0.6465520858764648,
      }
    };

    //    double[][] epsilonArray = new double[datasetNameList.length][];
    //    for (int i = 0; i < datasetNameList.length; i++) {
    //      epsilonArray[i] = new double[noutList.length];
    //    }

    for (int y = 0; y < datasetNameList.length; y++) {
      //      if (y != 1) {
      //        System.out.println("only for Qloss");
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
          //            continue; // because FSW on Qloss special
          //          }

          int nout = noutList[x];

          //          double epsilon = MySample_fsw2.getFSWParam(nout, ts, 1e-6);
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

    //    for (int i = 0; i < epsilonArray.length; i++) {
    //      for (int j = 0; j < epsilonArray[i].length; j++) {
    //        System.out.print(epsilonArray[i][j] + ",");
    //      }
    //      System.out.println();
    //    }

    // do not change name of the output file, as the output is used in exp bash
    try (FileWriter writer = new FileWriter("epsilonArray_fsw.txt")) {
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

  //  public static double getFSWParam(int nout, TimeSeries ts, double accuracy) throws IOException
  // {
  //    double epsilon = 1;
  //    boolean directLess = false;
  //    boolean directMore = false;
  //    boolean skip = false;
  //    int threshold = 2;
  //    while (true) {
  //      List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
  //      if (reducedPoints.size() > nout) {
  //        if (directMore) {
  //          if (Math.abs(reducedPoints.size() - nout) <= threshold) {
  //            skip = true;
  //          }
  //          break;
  //        }
  //        if (!directLess) {
  //          directLess = true;
  //        }
  //        epsilon *= 2;
  //      } else {
  //        if (directLess) {
  //          if (Math.abs(nout - reducedPoints.size()) <= threshold) {
  //            skip = true;
  //          }
  //          break;
  //        }
  //        if (!directMore) {
  //          directMore = true;
  //        }
  //        epsilon /= 2;
  //      }
  //    }
  //    if (skip) {
  //      return epsilon;
  //    }
  //
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
  //      List<Point> reducedPoints = FSW.reducePoints(ts.data, mid);
  //      if (reducedPoints.size() > nout) {
  //        left = mid;
  //      } else {
  //        right = mid;
  //      }
  //    }
  //
  //    List<Point> reducedPoints = FSW.reducePoints(ts.data, left);
  //    int n1 = reducedPoints.size();
  //    reducedPoints = FSW.reducePoints(ts.data, right);
  //    int n2 = reducedPoints.size();
  //    if (Math.abs(n1 - nout) < Math.abs(n2 - nout)) {
  //      return left;
  //    } else {
  //      return right;
  //    }
  //  }
}
