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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.stream.Collectors;

// adapted from the open source C++ code
// https://github.com/ofZach/Visvalingam-Whyatt/blob/master/src/testApp.cpp
class Triangle {

  int[] indices = new int[3];
  double area;
  Triangle prev;
  Triangle next;

  public Triangle(int index1, int index2, int index3, double area) {
    this.indices[0] = index1;
    this.indices[1] = index2;
    this.indices[2] = index3;
    this.area = area;
  }
}

class vPoint {

  double x, y, z;

  public vPoint(double x, double y) {
    this.x = x;
    this.y = y;
    this.z = Double.POSITIVE_INFINITY; // effective area
  }
}

class Polyline {

  private List<vPoint> vertices = new ArrayList<>();

  public void addVertex(vPoint point) {
    vertices.add(point);
  }

  public List<vPoint> getVertices() {
    return new ArrayList<>(vertices);
  }

  public int size() {
    return vertices.size();
  }

  public vPoint get(int index) {
    return vertices.get(index);
  }
}

public class Visval_standard {

  // 计算三角形面积
  private static double triArea(vPoint d0, vPoint d1, vPoint d2) {
    double dArea = ((d1.x - d0.x) * (d2.y - d0.y) - (d2.x - d0.x) * (d1.y - d0.y)) / 2.0;
    return (double) ((dArea > 0.0) ? dArea : -dArea); // abs
  }

  public static void buildEffectiveArea(Polyline lineToSimplify, List<vPoint> results) {
    results.clear();
    results.addAll(lineToSimplify.getVertices());

    int total = lineToSimplify.size();
    if (total < 3) {
      return; // 不足 3 个点无法形成三角形
    }

    int nTriangles = total - 2;
    Triangle[] triangles = new Triangle[nTriangles];

    // 创建所有三角形并计算初始面积
    for (int i = 1; i < total - 1; i++) {
      int index1 = i - 1, index2 = i, index3 = i + 1;
      double area =
          triArea(
              lineToSimplify.get(index1), lineToSimplify.get(index2), lineToSimplify.get(index3));
      triangles[i - 1] = new Triangle(index1, index2, index3, area);
    }

    // 设置三角形的前后关系
    for (int i = 0; i < nTriangles; i++) {
      triangles[i].prev = (i == 0 ? null : triangles[i - 1]);
      triangles[i].next = (i == nTriangles - 1 ? null : triangles[i + 1]);
    }

    // 使用优先队列构建 minHeap
    PriorityQueue<Triangle> triangleHeap =
        new PriorityQueue<>(Comparator.comparingDouble(t -> t.area));
    Collections.addAll(triangleHeap, triangles);

    double previousEA = -1;
    while (!triangleHeap.isEmpty()) {
      // 注意peek只需要直接访问该位置的元素，不涉及任何重排或堆化操作
      // 而poll是删除堆顶元素，需要重新堆化以维护堆的性质，复杂度是O(logk),k是当前堆的大小
      Triangle tri = triangleHeap.poll();

      // 更新当前点的重要性（z 轴存储effective area,这是一个单调增的指标）
      if (tri.area > previousEA) {
        previousEA = tri.area;
      }
      results.get(tri.indices[1]).z = previousEA;
      //      System.out.println(tri.indices[1] + "," + previousEA);

      // 更新相邻三角形
      if (tri.prev != null) {
        // 前一个三角形连到后一个三角形
        tri.prev.next = tri.next;
        tri.prev.indices[2] = tri.indices[2];
        tri.prev.area =
            triArea(
                lineToSimplify.get(tri.prev.indices[0]),
                lineToSimplify.get(tri.prev.indices[1]),
                lineToSimplify.get(tri.prev.indices[2]));

        // 重新加入堆
        // 在 Java 的 PriorityQueue 中，修改元素的属性不会自动更新堆的顺序
        // 所以必须通过 remove 和 add 来显式重新插入修改后的元素
        triangleHeap.remove(tri.prev);
        triangleHeap.add(tri.prev);
      }

      if (tri.next != null) {
        // 后一个三角形连到前一个三角形
        tri.next.prev = tri.prev;
        tri.next.indices[0] = tri.indices[0];
        tri.next.area =
            triArea(
                lineToSimplify.get(tri.next.indices[0]),
                lineToSimplify.get(tri.next.indices[1]),
                lineToSimplify.get(tri.next.indices[2]));

        // 重新加入堆
        triangleHeap.remove(tri.next);
        triangleHeap.add(tri.next);
      }
    }
  }

  public static void main(String[] args) {
    Polyline polyline = new Polyline();
    List<Polyline> polylineList = new ArrayList<>();
    Random rand = new Random();
    int n = 1000_000;
    int p = 10;
    for (int i = 0; i < n; i += p) {
      Polyline polylineBatch = new Polyline();
      for (int j = i; j < Math.min(i + p, n); j++) {
        double v = rand.nextInt(1000);
        polyline.addVertex(new vPoint(j, v));
        polylineBatch.addVertex(new vPoint(j, v));
      }
      polylineList.add(polylineBatch);
    }

    System.out.println("---------------------------------");
    List<vPoint> results = new ArrayList<>();
    // 计算运行时间
    long startTime = System.currentTimeMillis();
    buildEffectiveArea(polyline, results);
    // 输出结果
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken to reduce points: " + (endTime - startTime) + "ms");
    System.out.println(results.size());

    //    for (int i = 0; i < results.size(); i++) {
    //      vPoint point = results.get(i);
    //      System.out.println("Point: (" + point.x + ", " + point.y + ", " + point.z + ")");
    //    }

    System.out.println("---------------------------------");
    List<List<vPoint>> resultsBatchList = new ArrayList<>();
    // 计算运行时间
    int cnt = 0;
    startTime = System.currentTimeMillis();
    for (Polyline polylineBatch : polylineList) {
      List<vPoint> resultsBatch = new ArrayList<>();
      buildEffectiveArea(polylineBatch, resultsBatch);
      cnt += resultsBatch.size();
      resultsBatchList.add(resultsBatch);
    }
    // 输出结果
    endTime = System.currentTimeMillis();
    System.out.println("Time taken to reduce points: " + (endTime - startTime) + "ms");
    System.out.println(cnt);

    System.out.println("---------------------------------");
    // 使用 Stream API 合并所有列表
    List<vPoint> mergedList =
        resultsBatchList.stream().flatMap(List::stream).collect(Collectors.toList());
    int sameCnt = 0;
    for (int i = 0; i < mergedList.size(); i++) {
      if (mergedList.get(i).z == results.get(i).z) {
        sameCnt += 1;
      }
    }
    System.out.println("sameCnt=" + sameCnt + ", percent=" + sameCnt * 1.0 / mergedList.size());
  }

  //    float[] vlist = new float[]{11346, 33839, 35469, 23108, 22812, 5519, 5526, 4865, 5842,
  // 23089};
  //    for (int i = 0; i < vlist.length; i++) {
  //      polyline.addVertex(new vPoint(i, vlist[i]));
  //    }

  //    ArrayList<Double> v = new ArrayList<>();
  //    String filePath = "D://desktop//数据集//New York Stock Exchange//merged_prices.csv";
  //    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
  //      String line;
  //      int count = 0;
  //      while ((line = br.readLine()) != null && count < 1000) {
  //        String[] columns = line.split(","); // 假设 CSV 文件以逗号分隔
  //        v.add(Double.parseDouble(columns[0])); // 读取第一列数据
  //        count++;
  //      }
  //    } catch (Exception e) {
  //      e.printStackTrace();
  //    }
  //    // 打印数据长度
  //    System.out.println("Data length: " + v.size());
  //    for (int i = 0; i < v.size(); i++) {
  //      polyline.addVertex(new vPoint(i, v.get(i)));
  //    }
}
