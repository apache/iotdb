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

public class MySample_fsw_motivation {

  public static void main(String[] args) {
    String file = "tmp.csv";
    String out = "tmp-fsw.csv";
    int[] noutList = new int[] {7};
    double[] epsilonList = new double[] {3.342602252960205};

    for (int nout : noutList) {
      boolean hasHeader = false;
      try (FileInputStream inputStream = new FileInputStream(file)) {
        String delimiter = ",";
        TimeSeries ts =
            TimeSeriesReader.getMyTimeSeries(
                inputStream, delimiter, false, -1, 0, hasHeader, false);
        //        double epsilon = getFSWParam(nout, ts, 1e-6);
        double epsilon = epsilonList[0];
        List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
        System.out.println("epsilon=" + epsilon + ",actual m=" + reducedPoints.size());
        try (PrintWriter writer = new PrintWriter(new FileWriter(out))) {
          for (Point p : reducedPoints) {
            writer.println(p.getTimestamp() + "," + p.getValue());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public static double getFSWParam(int nout, TimeSeries ts, double accuracy) throws IOException {
    double epsilon = 1;
    boolean directLess = false;
    boolean directMore = false;
    while (true) {
      List<Point> reducedPoints = FSW.reducePoints(ts.data, epsilon);
      if (reducedPoints.size() > nout) {
        if (directMore) {
          break;
        }
        if (!directLess) {
          directLess = true;
        }
        epsilon *= 2;
      } else {
        if (directLess) {
          break;
        }
        if (!directMore) {
          directMore = true;
        }
        epsilon /= 2;
      }
    }
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
    return (left + right) / 2;
  }
}
