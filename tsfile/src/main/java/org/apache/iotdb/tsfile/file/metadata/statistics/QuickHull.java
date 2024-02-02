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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class QuickHull {

  public static BitSet quickHull(
      List<QuickHullPoint> points) { // TODO check this input points deep clone?
    BitSet bitSet = new BitSet(points.size());

    //    List<Point> convexHull = new ArrayList<>();

    if (points.size() < 3) {
      bitSet.set(0);
      bitSet.set(1);
      bitSet.set(2);
      return bitSet;
    }

    // assume point.x i.e., t is monotonically increasing
    int minPoint = 0;
    int maxPoint = points.size() - 1;
    //    bitSet.set(0);
    //    bitSet.set(points.size() - 1); // TODO check this has not remove any
    QuickHullPoint A = points.get(minPoint);
    QuickHullPoint B = points.get(maxPoint);
    //    convexHull.add(A);
    //    convexHull.add(B);
    //    points.remove(A); // TODO check this input data deep clone?
    //    points.remove(B);
    points.remove(A);
    points.remove(B); // TODO check this
    bitSet.set(A.idx);
    bitSet.set(B.idx);

    ArrayList<QuickHullPoint> leftSet = new ArrayList<>();
    ArrayList<QuickHullPoint> rightSet = new ArrayList<>();

    for (QuickHullPoint p : points) {
      if (pointLocation(A, B, p) == -1) {
        leftSet.add(p);
      } else if (pointLocation(A, B, p) == 1) {
        rightSet.add(p);
      }
    }
    hullSet(A, B, rightSet, bitSet);
    hullSet(B, A, leftSet, bitSet);

    return bitSet;
  }

  public static double distance(QuickHullPoint A, QuickHullPoint B, QuickHullPoint C) {
    double ABx = B.t - A.t;
    double ABy = B.v - A.v;
    double num = ABx * (A.v - C.v) - ABy * (A.t - C.t);
    if (num < 0) {
      num = -num;
    }
    return num;
    // 返回的是ABC三角形面积的二倍数
  }

  public static void hullSet(
      QuickHullPoint A, QuickHullPoint B, List<QuickHullPoint> set, BitSet bitSet) {
    //    int insertPosition = hull.indexOf(B);
    if (set.size() == 0) {
      return;
    }
    if (set.size() == 1) {
      QuickHullPoint p = set.get(0);
      set.remove(p);
      //      hull.add(insertPosition, p);
      bitSet.set(p.idx);
      return;
    }
    double dist = -Double.MAX_VALUE;
    //    int furthestPoint = -1;
    QuickHullPoint furthestPoint = null;
    for (QuickHullPoint p : set) {
      double distance = distance(A, B, p);
      if (distance > dist) {
        dist = distance;
        furthestPoint = p;
      }
    }
    //    QuickHullPoint P = set.get(furthestPoint);
    set.remove(furthestPoint);
    //    hull.add(insertPosition, P);
    bitSet.set(furthestPoint.idx);

    // Determine who's to the left of AP
    ArrayList<QuickHullPoint> leftSetAP = new ArrayList<>();
    for (QuickHullPoint M : set) {
      if (pointLocation(A, furthestPoint, M) == 1) {
        leftSetAP.add(M);
      }
    }

    // Determine who's to the left of PB
    ArrayList<QuickHullPoint> leftSetPB = new ArrayList<>();
    for (QuickHullPoint M : set) {
      if (pointLocation(furthestPoint, B, M) == 1) {
        leftSetPB.add(M);
      }
    }
    hullSet(A, furthestPoint, leftSetAP, bitSet);
    hullSet(furthestPoint, B, leftSetPB, bitSet);
  }

  public static int pointLocation(QuickHullPoint A, QuickHullPoint B, QuickHullPoint P) {
    double cp1 = (B.t - A.t) * (P.v - A.v) - (B.v - A.v) * (P.t - A.t);
    return Double.compare(cp1, 0);
    // return 1 means P is above the line connecting A and B
    // return -1 means P is below the line connecting A and B
    // return 0 means P is on the line connecting A and B
  }
}
