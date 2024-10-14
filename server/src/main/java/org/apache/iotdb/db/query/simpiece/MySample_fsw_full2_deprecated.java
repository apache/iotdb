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

public class MySample_fsw_full2_deprecated {
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
        9.618214055872158,
        8.99153642953479,
        8.375,
        7.994615384615827,
        7.615596330275366,
        7.172755905511622,
        6.8775202380129485,
        6.408134920635348,
        6.219821826281077,
        5.977595190380271,
        5.678260869564838,
        5.499468085106855,
        5.387459854014196,
        5.299999999999272,
        5.1635087719305375,
      },
      {
        9.99588181002764E-4, 9.9792019045708E-4, 9.960159368347377E-4, 9.93464052953641E-4,
        9.900738350552274E-4, 9.817607760851388E-4, 9.688081945569138E-4, 9.424000008948497E-4,
        8.978153637144715E-4, 8.259669120889157E-4, 6.680341875835438E-4, 5.191545578782097E-4,
        4.996619345547515E-4, 4.989106755601824E-4, 4.871794872087776E-4,
      },
      {
        439.5207473666851, 425.8416748119398, 405.7711305285957, 396.33365325326304,
        380.677698415484, 358.4453466879504, 336.54477008704634, 304.11544481893816,
        279.7037549556544, 257.61061704471194, 232.59544346653, 211.35450069313174,
        201.14805889728268, 192.1216488917953, 178.6315376164921,
      },
      {
        9.297997111218137, 7.471379310345583, 6.473535325328157, 5.616272926722559,
        4.701954477981417, 3.7258684097114383, 3.078884904984079, 2.4637460027415727,
        2.0368421052635313, 1.7683037779497681, 1.473529411765412, 1.2658022184905349,
        1.166260869566031, 1.0711538461546297, 0.9613079019081852,
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

          //          double epsilon = MySample_fsw.getFSWParam(nout, ts, 1e-12);
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
