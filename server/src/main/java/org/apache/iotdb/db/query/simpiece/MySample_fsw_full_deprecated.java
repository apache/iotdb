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

public class MySample_fsw_full_deprecated {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList = new int[] {320, 360, 400, 440, 480, 520, 560, 600, 640};

    double[][] epsilonArray = {
      {
        9.618271350860596,
        9.101282596588135,
        8.991526126861572,
        8.62817907333374,
        8.374999523162842,
        8.202636241912842,
        7.999906063079834,
        7.947059154510498,
        7.858933925628662
      },
      {
        9.999275207519531E-4,
        9.989738464355469E-4,
        9.980201721191406E-4,
        9.970664978027344E-4,
        9.961128234863281E-4,
        9.951591491699219E-4,
        9.942054748535156E-4,
        9.932518005371094E-4,
        9.922981262207031E-4
      },
      {
        440.0235962867737,
        432.0247492790222,
        423.55674982070923,
        414.65001153945923,
        405.77113008499146,
        401.21820974349976,
        396.7935194969177,
        393.2065939903259,
        392.30953645706177
      },
      {
        9.295397281646729,
        8.096678256988525,
        7.536666393280029,
        6.961357593536377,
        6.473535060882568,
        6.064479351043701,
        5.743286609649658,
        5.5219340324401855,
        5.185042858123779
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
          //          double epsilon = MySample_fsw.getFSWParam(nout, ts, 1e-6);
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
}
