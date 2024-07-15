// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
// Sim-Piece code forked from https://github.com/xkitsios/Sim-Piece.git

package org.apache.iotdb.db.query.simpiece;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;

public class MySample_visval {

  public static void main(String[] args) {
    String fileDir = "D:\\desktop\\NISTPV\\";
    String[] datasetNameList =
        new String[] {
          "NISTPV-Ground-2015-Qloss_Ah",
          //            "NISTPV-Ground-2015-Pyra1_Wm2",
          //          "NISTPV-Ground-2015-RTD_C_3"
        };

    int[] noutList = new int[] {2400000};

    double[] r = new double[] {0.1, 0.5, 1.3};
    for (int y = 0; y < datasetNameList.length; y++) {
      String datasetName = datasetNameList[y];
      int start = (int) (10000000 / 2 - 2500000 * r[y]); // 从0开始计数
      int end = (int) (10000000 / 2 + 2500000 * (1 - r[y]));
      int N = end - start; // -1 for all
      //
      //      int start = 0;
      //      int end = 100000;
      //      int N = end - start;

      for (int nout : noutList) {
        // apply Sim-Piece on the input file, outputting nout points saved in csvFile
        boolean hasHeader = false;
        try (FileInputStream inputStream = new FileInputStream(fileDir + datasetName + ".csv")) {
          String delimiter = ",";
          List<VisvalPoint> points =
              TimeSeriesReader.getMyTimeSeriesVisval(
                  inputStream, delimiter, false, N, start, hasHeader, false);
          System.out.println(points.size());
          System.out.println(nout);

          List<VisvalPoint> reducedPoints = Visval.reducePoints(points, nout);
          try (PrintWriter writer =
              new PrintWriter(
                  new FileWriter(
                      datasetName + "-" + N + "-" + reducedPoints.size() + "-visval.csv"))) {
            for (VisvalPoint p : reducedPoints) {
              writer.println(p.x + "," + p.y);
            }
          }
          System.out.println(reducedPoints.size());
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  //          List<Point> points = new ArrayList<>();
  //          points.add(new Point(1, 10));
  //          points.add(new Point(2, 20));
  //          points.add(new Point(3, 15));
  //          points.add(new Point(4, 10));
  //          points.add(new Point(5, 30));
  //          points.add(new Point(6, 25));
  //          points.add(new Point(7, 20));
  //          int m = 4;
  //          List<Point> reducedPoints = Visval.reducePoints(points, m);
  //          System.out.println("Reduced points:");
  //          for (Point point : reducedPoints) {
  //            System.out.println("Timestamp: " + point.getTimestamp() + ", Value: " +
  // point.getValue());
  //          }
}
