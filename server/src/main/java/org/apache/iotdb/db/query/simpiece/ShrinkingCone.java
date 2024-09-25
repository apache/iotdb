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

package org.apache.iotdb.db.query.simpiece;

import org.apache.iotdb.tsfile.read.common.IOMonitor2;

import java.util.ArrayList;
import java.util.List;

public class ShrinkingCone {

  public static List<Point> reducePoints(List<Point> points, double epsilon) {
    List<Point> result = new ArrayList<>();
    int length = points.size();

    result.add(points.get(0)); // first point

    // Precompute upper and lower bounds
    double[] p_upper = new double[length];
    double[] p_lower = new double[length];
    for (int j = 0; j < length; j++) {
      double v = points.get(j).getValue();
      p_upper[j] = v + epsilon;
      p_lower[j] = v - epsilon;
    }

    // init the first segment
    int sp = 0;
    int i = 1;
    IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
    double vsp = points.get(sp).getValue();
    double dx = points.get(i).getTimestamp() - points.get(sp).getTimestamp();
    double upSlope = (p_upper[i] - vsp) / dx;
    double lowSlope = (p_lower[i] - vsp) / dx;

    while (i < length - 1) {
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      i++;
      vsp = points.get(sp).getValue(); // the value of the start point
      dx = points.get(i).getTimestamp() - points.get(sp).getTimestamp(); // time distance
      double upV = upSlope * dx + vsp; // the upper bound for the current point
      double lowV = lowSlope * dx + vsp; // the lower bound for the current point

      if (lowV <= points.get(i).getValue() && points.get(i).getValue() <= upV) {
        // current point in the zone
        // shrink the zone
        upSlope = Math.min(upSlope, (p_upper[i] - vsp) / dx);
        lowSlope = Math.max(lowSlope, (p_lower[i] - vsp) / dx);
      } else {
        // record segment point
        result.add(points.get(i - 1));

        // begin new segment
        IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
        sp = i - 1; // joint style
        vsp = points.get(sp).getValue(); // note sp has changed
        dx = points.get(i).getTimestamp() - points.get(sp).getTimestamp(); // note sp has changed
        upSlope = (p_upper[i] - vsp) / dx;
        lowSlope = (p_lower[i] - vsp) / dx;
      }
    }

    // write last point
    IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
    result.add(points.get(points.size() - 1));

    return result;
  }
}
