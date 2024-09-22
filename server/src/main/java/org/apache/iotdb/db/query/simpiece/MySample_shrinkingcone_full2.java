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

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList =
        new int[] {
          320, 400, 480, 580, 720, 960, 1200, 1600, 2000, 2400, 3000, 3600, 4000, 4400, 5000
        };

    double[][] epsilonArray = {
      {
        14.388325691223145, 13.343345642089844, 12.697674751281738, 12.053359985351562,
        11.411823272705078, 10.700932502746582, 10.222302436828613, 9.640463829040527,
        9.169320106506348, 8.824727058410645, 8.389120101928711, 8.050561904907227,
        7.84444522857666, 7.675000190734863, 7.441162109375,
      },
      {
        0.001, 0.001, 0.001, 0.001,
        0.001, 0.001, 0.001, 9.9945068359375E-4,
        9.9945068359375E-4, 9.9945068359375E-4, 9.984970092773438E-4, 9.946823120117188E-4,
        9.927749633789062E-4, 9.8419189453125E-4, 9.088516235351562E-4,
      },
      {
        680.2196474075317, 650.261082649231, 620.9016036987305, 586.952600479126,
        547.2381553649902, 493.3626317977905, 453.35704135894775, 404.6129741668701,
        368.50129795074463, 342.09704971313477, 305.65501976013184, 276.0216178894043,
        257.88243865966797, 242.78428554534912, 223.49278926849365,
      },
      {
        11.259881019592285, 9.402754783630371, 8.384316444396973, 7.238059043884277,
        5.794983863830566, 4.60251522064209, 3.8171539306640625, 3.0447826385498047,
        2.526477813720703, 2.1706619262695312, 1.816070556640625, 1.5562095642089844,
        1.4302129745483398, 1.3176708221435547, 1.1894960403442383,
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

          //          double epsilon = getSCParam(nout, ts, 1e-15);
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

  public static double getSCParam(int nout, TimeSeries ts, double accuracy) throws IOException {
    double epsilon = 1;
    boolean directLess = false;
    boolean directMore = false;
    boolean skip = false; // TODO
    int threshold = 2; // TODO
    while (true) {
      List<Point> reducedPoints = ShrinkingCone.reducePoints(ts.data, epsilon);
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
      List<Point> reducedPoints = ShrinkingCone.reducePoints(ts.data, mid);
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
