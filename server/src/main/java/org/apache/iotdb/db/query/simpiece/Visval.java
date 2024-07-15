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

import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class Visval {

  public static List<VisvalPoint> reducePoints(List<VisvalPoint> points, int targetCount) {
    if (points.size() <= targetCount) {
      return points;
    }

    ConcurrentSkipListMap<Double, VisvalPoint> minHeap = new ConcurrentSkipListMap<>();
    // 初始化双向链表和优先队列
    for (int i = 1; i < points.size() - 1; i++) {
      VisvalPoint p = points.get(i);
      p.prev = points.get(i - 1);
      p.next = points.get(i + 1);
      p.area = calculateArea(p.prev, p, p.next);
      minHeap.put(p.area, p);
    }

    while (points.size() > targetCount) {
      // 移除面积最小的点
      VisvalPoint p = minHeap.pollFirstEntry().getValue();
      if (p == null) {
        return points;
      }
      // 移除点 p
      VisvalPoint prev = p.prev;
      VisvalPoint next = p.next;
      prev.next = next;
      next.prev = prev;

      // 更新相邻点的三角形面积
      if (prev.prev != null) {
        minHeap.remove(prev.area);
        prev.area = calculateArea(prev.prev, prev, next);
        minHeap.put(prev.area, prev);
      }
      if (next.next != null) {
        minHeap.remove(next.area);
        next.area = calculateArea(prev, next, next.next);
        minHeap.put(next.area, next);
      }
      points.remove(p);
    }

    return points;
  }

  private static double calculateArea(VisvalPoint a, VisvalPoint b, VisvalPoint c) {
    // 计算三角形面积的函数
    return Math.abs(a.x * (b.y - c.y) + b.x * (c.y - a.y) + c.x * (a.y - b.y)) / 2.0;
  }

  //  public static void main(String[] args) {
  //    List<VisvalPoint> points = new ArrayList<>();
  //    List<Point> pointsold = new ArrayList<>();
  //    // 添加点到列表 points 100万数据
  //    Random rand = new Random();
  //
  //    int targetCount = 10;
  //    for (int i = 0; i < 10000; i++) {
  //      int v = rand.nextInt(1000);
  //      points.add(new VisvalPoint(i, v));
  //      pointsold.add(new Point(i, v));
  //    }
  //
  ////    int targetCount = 9000000;
  ////    for (int i = 0; i < 10000000; i++) {
  ////      points.add(new VisvalPoint(i, rand.nextInt(1000)));
  ////    }
  //
  //    // 计算运行时间
  //    long startTime = System.currentTimeMillis();
  //
  //    List<VisvalPoint> reducedPoints = Visval.reducePoints(points, targetCount);
  //    for (VisvalPoint point : reducedPoints) {
  //      System.out.println("Timestamp: " + point.x + ", Value: " + point.y);
  //    }
  //    List<Point> reducedPointsold = Visvalold.reducePoints(pointsold, targetCount);
  //    for (Point point : reducedPointsold) {
  //      System.out.println("Timestamp: " + point.getTimestamp() + ", Value: " +
  //          point.getValue());
  //    }
  //    // 输出结果
  //    System.out.println(reducedPoints.size());
  //    long endTime = System.currentTimeMillis();
  //    System.out.println("Time taken to reduce points: " + (endTime - startTime) + "ms");
  //  }
}
