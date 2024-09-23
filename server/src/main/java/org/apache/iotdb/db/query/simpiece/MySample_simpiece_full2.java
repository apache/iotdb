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
import java.util.Comparator;
import java.util.List;

public class MySample_simpiece_full2 {
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
        10.421952322125435, 9.571347452700138, 9.122375220060349, 8.709278352558613,
        8.077139288187027, 7.495633184909821, 7.083636365830898, 6.733693778514862,
        6.403274565935135, 6.160000003874302, 5.93758013099432, 5.787152819335461,
        5.647759035229683, 5.5742857083678246, 5.444876328110695,
      },
      {
        9.987801313400269E-4, 9.798482060432434E-4, 9.232386946678162E-4, 8.238255977630615E-4,
        7.013455033302307E-4, 6.666630506515503E-4, 6.311237812042236E-4, 5.687475204467773E-4,
        5.405843257904053E-4, 5.235522985458374E-4, 5.015432834625244E-4, 4.99919056892395E-4,
        4.997774958610535E-4, 4.995688796043396E-4, 4.983171820640564E-4,
      },
      {
        502.05052164942026, 485.91779258847237, 464.11991154402494, 436.75185588002205,
        411.17336819320917, 383.33036886900663, 366.5155046656728, 334.26181526482105,
        308.3470098376274, 288.7738408744335, 260.332594871521, 242.4736728295684,
        229.66440346837044, 219.4005109295249, 205.81142588704824,
      },
      {
        13.637416116893291, 11.293272890150547, 9.741776660084724, 8.293842449784279,
        6.9908598735928535, 5.493516407907009, 4.693962275981903, 3.6259122267365456,
        2.9906081929802895, 2.5887924432754517, 2.1600354313850403, 1.8493054434657097,
        1.689537525177002, 1.5635392591357231, 1.3930133953690529,
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
          //
          //          double epsilon = getSimPieceParam(nout, ts, 1e-8);
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

//    for (int i = 0; i < epsilonArray.length; i++) { // 遍历行
//      for (int j = 0; j < epsilonArray[i].length; j++) { // 遍历列
//        System.out.print(epsilonArray[i][j] + ",");
//      }
//      System.out.println();
//    }

    // do not change name of the output file, as the output is used in exp bash
    try (FileWriter writer = new FileWriter("epsilonArray_simpiece.txt")) {
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

  public static double getSimPieceParam(int nout, TimeSeries ts, double accuracy)
      throws IOException {
    double epsilon = 1;
    boolean directLess = false;
    boolean directMore = false;
    boolean skip = false; // TODO
    int threshold = 2; // TODO
    while (true) {
      SimPiece simPiece = new SimPiece(ts.data, epsilon);
      if (simPiece.segments.size() * 2 > nout) { // note *2 for disjoint
        if (directMore) {
          // TODO
          if (Math.abs(simPiece.segments.size() * 2 - nout) <= threshold) {
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
          if (Math.abs(nout - simPiece.segments.size() * 2) <= threshold) {
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
      SimPiece simPiece = new SimPiece(ts.data, mid);
      if (simPiece.segments.size() * 2 > nout) { // note *2 for disjoint
        left = mid;
      } else {
        right = mid;
      }
    }
    // TODO
    SimPiece simPiece = new SimPiece(ts.data, left);
    int n1 = simPiece.segments.size() * 2;
    simPiece = new SimPiece(ts.data, right);
    int n2 = simPiece.segments.size() * 2;
    if (Math.abs(n1 - nout) < Math.abs(n2 - nout)) {
      return left;
    } else {
      return right;
    }
    //
  }
}
