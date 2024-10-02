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
import java.util.Comparator;
import java.util.List;

public class MySample_simpiece_full_deprecated {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList = new String[] {"WindSpeed", "Qloss", "Pyra1", "RTD"};
    int[] noutList = new int[] {320, 360, 400, 440, 480, 520, 560, 600, 640};

    double[][] epsilonArray = {
      {
        10.470000203704831,
        9.973635465240477,
        9.571347576141356,
        9.339938166046142,
        9.208912633514405,
        9.135163892364506,
        8.744444478607178,
        8.61042620162964,
        8.422101323699948
      },
      {
        9.987499999999999E-4,
        9.974999999999997E-4,
        9.784999999999998E-4,
        9.4425E-4,
        9.255E-4,
        8.8375E-4,
        8.4775E-4,
        7.865E-4,
        7.3825E-4
      },
      {
        502.0505219552564,
        493.3274919509581,
        485.91779255051347,
        479.26180967157245,
        463.5216509033579,
        446.88677012006065,
        441.2913785749938,
        435.45897277584766,
        426.0481592410648
      },
      {
        13.678959740142822,
        12.275238493499756,
        11.224719192962645,
        10.475999729766848,
        9.741776887359617,
        9.125823136444094,
        8.625197060394289,
        8.105121862335203,
        7.743280215911865
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
      // apply Sim-Piece on the input file, outputting nout points saved in csvFile
      boolean hasHeader = false;
      try (FileInputStream inputStream = new FileInputStream(fileDir + datasetName + ".csv")) {
        String delimiter = ",";
        TimeSeries ts =
            TimeSeriesReader.getMyTimeSeries(
                inputStream, delimiter, false, N, start, hasHeader, false);
        for (int x = 0; x < noutList.length; x++) {
          int nout = noutList[x];

          //          double epsilon = MySample_simpiece.getSimPieceParam(nout, ts, 1e-6);
          //          epsilonArray[y][x] = epsilon;

          double epsilon = epsilonArray[y][x];

          SimPiece simPiece = new SimPiece(ts.data, epsilon);
          System.out.println(
              datasetName
                  + ": n="
                  + N
                  + ",m="
                  + nout
                  + ",epsilon="
                  + epsilon
                  + ",actual m="
                  + simPiece.segments.size() * 2);
          List<SimPieceSegment> segments = simPiece.segments;
          segments.sort(Comparator.comparingLong(SimPieceSegment::getInitTimestamp));
          try (PrintWriter writer =
              new PrintWriter(
                  new FileWriter(
                      datasetName
                          + "-"
                          + N
                          + "-"
                          + nout
                          + "-"
                          + segments.size() * 2
                          + "-simpiece.csv"))) {
            for (int i = 0; i < segments.size() - 1; i++) {
              // start point of this segment
              writer.println(segments.get(i).getInitTimestamp() + "," + segments.get(i).getB());
              // end point of this segment
              double v =
                  (segments.get(i + 1).getInitTimestamp() - segments.get(i).getInitTimestamp())
                          * segments.get(i).getA()
                      + segments.get(i).getB();
              writer.println(segments.get(i + 1).getInitTimestamp() + "," + v);
            }
            // the two end points of the last segment
            writer.println(
                segments.get(segments.size() - 1).getInitTimestamp()
                    + ","
                    + segments.get(segments.size() - 1).getB());
            double v =
                (simPiece.lastTimeStamp - segments.get(segments.size() - 1).getInitTimestamp())
                        * segments.get(segments.size() - 1).getA()
                    + segments.get(segments.size() - 1).getB();
            writer.println(simPiece.lastTimeStamp + "," + v);
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

  //  public static double getSimPieceParam(int nout, TimeSeries ts, double accuracy)
  //      throws IOException {
  //    double epsilon = ts.range * 0.001;
  //    while (true) {
  //      SimPiece simPiece = new SimPiece(ts.data, epsilon);
  //      if (simPiece.segments.size() * 2 > nout) { // note *2 for disjoint
  //        epsilon *= 2;
  //      } else {
  //        break;
  //      }
  //    }
  //    double left = epsilon / 2;
  //    double right = epsilon;
  //    while (Math.abs(right - left) > accuracy) {
  //      double mid = (left + right) / 2;
  //      SimPiece simPiece = new SimPiece(ts.data, mid);
  //      if (simPiece.segments.size() * 2 > nout) { // note *2 for disjoint
  //        left = mid;
  //      } else {
  //        right = mid;
  //      }
  //    }
  //    return (left + right) / 2;
  //  }
}
