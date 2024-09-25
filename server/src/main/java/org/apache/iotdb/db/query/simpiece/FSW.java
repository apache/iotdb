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

// Liu, Xiaoyan, Lin, Zhenjiang, Wang, Huaiqing, 2008. Novel online methods for time series
// segmentation.
// FSW: feasible space window
// adapted from https://github.com/zhourongleiden/AEPLA.git
public class FSW {

  public static List<Point> reducePoints(List<Point> points, double epsilon) {
    int length = points.size();

    // Precompute upper and lower bounds
    double[] p_upper = new double[length];
    double[] p_lower = new double[length];
    for (int j = 0; j < length; j++) {
      double v = points.get(j).getValue();
      p_upper[j] = v + epsilon;
      p_lower[j] = v - epsilon;
    }

    // init the first segment
    int i = 0;
    int seg_no = 0;
    int csp_id = 0;
    List<int[]> segmentPoint = new ArrayList<>();
    segmentPoint.add(new int[] {0, 0});
    double upSlope = Double.POSITIVE_INFINITY;
    double lowSlope = Double.NEGATIVE_INFINITY;

    while (i < length - 1) {
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      i++;
      upSlope =
          Math.min(
              upSlope,
              ((p_upper[i] - points.get(segmentPoint.get(seg_no)[1]).getValue())
                  / (points.get(i).getTimestamp()
                      - points.get(segmentPoint.get(seg_no)[1]).getTimestamp())));
      lowSlope =
          Math.max(
              lowSlope,
              ((p_lower[i] - points.get(segmentPoint.get(seg_no)[1]).getValue())
                  / (points.get(i).getTimestamp()
                      - points.get(segmentPoint.get(seg_no)[1]).getTimestamp())));
      if (upSlope < lowSlope) {
        seg_no += 1;
        segmentPoint.add(new int[] {seg_no, csp_id});
        i = csp_id;
        upSlope = Double.POSITIVE_INFINITY;
        lowSlope = Double.NEGATIVE_INFINITY;
      } else {
        double s =
            (points.get(i).getValue() - points.get(segmentPoint.get(seg_no)[1]).getValue())
                / (points.get(i).getTimestamp()
                    - points.get(segmentPoint.get(seg_no)[1]).getTimestamp());
        if (s >= lowSlope && s <= upSlope) {
          csp_id = i;
        }
      }
    }

    // deal with the last segment
    if (segmentPoint.get(seg_no)[1] < length - 1) {
      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      seg_no += 1;
      segmentPoint.add(new int[] {seg_no, length - 1});
    }

    List<Point> result = new ArrayList<>();
    for (int[] ints : segmentPoint) {
      result.add(points.get(ints[1]));
    }

    return result;
  }
}
