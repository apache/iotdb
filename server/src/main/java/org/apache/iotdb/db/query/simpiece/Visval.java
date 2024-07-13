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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

//    Visvalingam-Whyatt method of poly-line vertex reduction
//
//    Visvalingam, M and Whyatt J D (1993)
//    "Line Generalisation by Repeated Elimination of Points", Cartographic J., 30 (1), 46 - 51
//
//    Described here:
//
// http://web.archive.org/web/20100428020453/http://www2.dcs.hull.ac.uk/CISRG/publications/DPs/DP10/DP10.html
//
//    =========================================
//
//    The MIT License (MIT)
//
//    Copyright (c) 2014 Elliot Hallmark
//
//    Permission is hereby granted, free of charge, to any person obtaining a copy
//    of this software and associated documentation files (the "Software"), to deal
//    in the Software without restriction, including without limitation the rights
//    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//    copies of the Software, and to permit persons to whom the Software is
//    furnished to do so, subject to the following conditions:
//
//    The above copyright notice and this permission notice shall be included in all
//    copies or substantial portions of the Software.
//
//    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//    SOFTWARE.
//
//    ================================
public class Visval {

  private static class Triangle implements Comparable<Triangle> {

    int index;
    double area;

    Triangle(int index, double area) {
      this.index = index;
      this.area = area;
    }

    @Override
    public int compareTo(Triangle other) {
      return Double.compare(this.area, other.area);
    }
  }

  public static List<Point> reducePoints(List<Point> points, int m) {
    if (points.size() <= m) {
      return points;
    }

    // areas and remainIdx: records the dominating areas and indexes of remaining points during
    // bottom-up elimination
    // through remainIdx we can know the adjacency of remaining points easily.
    List<Double> areas = triangleAreaList(points);
    List<Integer> remainIdx = new ArrayList<>();
    IntStream.range(0, points.size()).forEach(remainIdx::add);

    int minIndex = 0;
    for (int i = 1; i < areas.size(); i++) {
      if (areas.get(i) < areas.get(minIndex)) {
        minIndex = i;
      }
    }
    double this_area = areas.get(minIndex);
    areas.remove(minIndex);
    remainIdx.remove(minIndex);

    while (remainIdx.size() > m) {
      boolean skip =
          false; // false mean next round needs to argmin globally, otherwise use recentMinIdx
      int recentMinIdx = -1;

      // update right new triangle area
      double right_area = Double.POSITIVE_INFINITY;
      if (minIndex <= remainIdx.size() - 2) {
        // note that now i already pop out min_vert
        right_area =
            calculateTriangleArea(
                points.get(remainIdx.get(minIndex - 1)),
                points.get(remainIdx.get(minIndex)),
                points.get(remainIdx.get(minIndex + 1)));
        if (right_area <= this_area) {
          // so next round does not need argmin globally
          skip = true;
          recentMinIdx = minIndex; // note that now i already pop out min_vert
        }
        areas.set(minIndex, right_area);
      }

      // update left new triangle area
      if (minIndex >= 2) {
        // note that now i already pop out min_vert
        double left_area =
            calculateTriangleArea(
                points.get(remainIdx.get(minIndex - 2)),
                points.get(remainIdx.get(minIndex - 1)),
                points.get(remainIdx.get(minIndex)));
        if (left_area <= this_area) {
          if (skip) { // means right point area is smaller than this_area, then compare left and
            // right
            if (left_area <= right_area) {
              recentMinIdx = minIndex - 1;
            }
            // otherwise keep skip right point
          } else { // means right point area is larger than this_area, while left is smaller than
            skip = true; // so next round does not need argmin globally
            recentMinIdx = minIndex - 1;
          }
        }
        areas.set(minIndex - 1, left_area);
      }

      if (!skip) { // left and right new triangle both larger than this_area, needs to argmin
        // globally
        minIndex = 0;
        for (int i = 1; i < areas.size(); i++) {
          if (areas.get(i) < areas.get(minIndex)) {
            minIndex = i;
          }
        }
      } else {
        minIndex = recentMinIdx;
      }
      this_area = areas.get(minIndex);
      areas.remove(minIndex);
      remainIdx.remove(minIndex);
    }

    List<Point> result = new ArrayList<>();
    for (int i : remainIdx) {
      result.add(points.get(i));
    }
    return result;
  }

  private static List<Double> triangleAreaList(List<Point> points) {
    List<Double> result = new ArrayList<>();
    result.add(Double.POSITIVE_INFINITY); // first
    for (int i = 1; i < points.size() - 1; i++) {
      double area = calculateTriangleArea(points.get(i - 1), points.get(i), points.get(i + 1));
      result.add(area);
    }
    result.add(Double.POSITIVE_INFINITY); // last
    return result;
  }

  private static double calculateTriangleArea(Point a, Point b, Point c) {
    double x1 = a.getTimestamp();
    double y1 = a.getValue();
    double x2 = b.getTimestamp();
    double y2 = b.getValue();
    double x3 = c.getTimestamp();
    double y3 = c.getValue();

    return Math.abs((x1 * (y2 - y3) + x2 * (y3 - y1) + x3 * (y1 - y2)) / 2.0);
  }
}
