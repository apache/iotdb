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
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

public class VisvalOld {

  public static List<VisvalPoint> reducePoints(List<VisvalPoint> points, int targetCount) {
    if (points.size() <= targetCount) {
      return points;
    }

    ConcurrentSkipListMap<Double, VisvalPoint> minHeap = new ConcurrentSkipListMap<>();
    for (int i = 1; i < points.size() - 1; i++) {
      VisvalPoint p = points.get(i);
      p.prev = points.get(i - 1);
      p.next = points.get(i + 1);
      p.area = calculateArea(p.prev, p, p.next);
      minHeap.put(p.area, p);
    }

    while (points.size() > targetCount) {
      // 这里有个问题：points remove和heap不对齐，因为有可能heap里有多个点的area相同就一起被remove了，但是points里还没有remove
      System.out.println(points.size());
      VisvalPoint p = minHeap.pollFirstEntry().getValue();
      if (p == null) {
        return points;
      }
      VisvalPoint prev = p.prev;
      VisvalPoint next = p.next;
      prev.next = next;
      next.prev = prev;

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
    return Math.abs(a.x * (b.y - c.y) + b.x * (c.y - a.y) + c.x * (a.y - b.y)) / 2.0;
  }

  public static void main(String[] args) {
    List<VisvalPoint> points = new ArrayList<>();
    // 添加点到列表 points 100万数据
    Random rand = new Random();
    int n = 10000;
    int m = 1;
    for (int i = 0; i < n; i++) {
      points.add(new VisvalPoint(i, rand.nextInt(1000)));
    }
    // 计算运行时间
    long startTime = System.currentTimeMillis();
    List<VisvalPoint> reducedPoints = VisvalOld.reducePoints(points, m);
    // 输出结果
    System.out.println(reducedPoints.size());
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken to reduce points: " + (endTime - startTime) + "ms");
  }
}
