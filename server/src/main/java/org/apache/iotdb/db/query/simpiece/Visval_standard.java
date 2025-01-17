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

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

// adapted from the open source C++ code
// https://github.com/ofZach/Visvalingam-Whyatt/blob/master/src/testApp.cpp
class Triangle {

    int[] indices = new int[3];
    double area;
    Triangle prev;
    Triangle next;
    boolean isDeleted;

    public Triangle(int index1, int index2, int index3, double area) {
        this.indices[0] = index1;
        this.indices[1] = index2;
        this.indices[2] = index3;
        this.area = area;
        this.isDeleted = false; // flag for removal. Avoid using heap.remove(x) as it is O(n) complexity
    }

    public Triangle(Triangle oldTri) {
        // deep copy and inherit connection

        this.indices[0] = oldTri.indices[0];
        this.indices[1] = oldTri.indices[1];
        this.indices[2] = oldTri.indices[2];
        this.area = oldTri.area;
        this.prev = oldTri.prev;
        this.next = oldTri.next;

        // TODO important! inherit connection relationship to this new point
        if (this.prev != null) { // previous point to this new point
            this.prev.next = this;
        }
        if (this.next != null) { // next point to this new point
            this.next.prev = this;
        }

        this.isDeleted = false; // this new triangle is not deleted
    }

    public boolean isValid() {
        return !isDeleted;
    }

    public void markDeleted() {
        this.isDeleted = true;
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
        results.addAll(lineToSimplify.getVertices()); // TODO debug

        int total = lineToSimplify.size();
        if (total < 3) {
            return; // 不足 3 个点无法形成三角形
        }

        int nTriangles = total - 2;
        Triangle[] triangles = new Triangle[nTriangles];

        // 创建所有三角形并计算初始面积
        for (int i = 1; i < total - 1; i++) {
            int index1 = i - 1, index2 = i, index3 = i + 1;
            double area = triArea(lineToSimplify.get(index1), lineToSimplify.get(index2), lineToSimplify.get(index3));
            triangles[i - 1] = new Triangle(index1, index2, index3, area);
        }

        // 设置三角形的前后关系
        for (int i = 0; i < nTriangles; i++) { // TODO 这个可以和上一个循环合并吗？？好像不可以
            triangles[i].prev = (i == 0 ? null : triangles[i - 1]);
            triangles[i].next = (i == nTriangles - 1 ? null : triangles[i + 1]);
        }

        // 使用优先队列构建 minHeap
        PriorityQueue<Triangle> triangleHeap = new PriorityQueue<>(Comparator.comparingDouble(t -> t.area));
        Collections.addAll(triangleHeap, triangles); // complexity TODO

        double previousEA = -1;
        while (!triangleHeap.isEmpty()) {
            // 注意peek只需要直接访问该位置的元素，不涉及任何重排或堆化操作
            // 而poll是删除堆顶元素，需要重新堆化以维护堆的性质，复杂度是O(logk),k是当前堆的大小
            Triangle tri = triangleHeap.poll(); // O(logn)

            // 如果该三角形已经被删除，跳过. Avoid using heap.remove(x) as it is O(n) complexity
            // 而且除了heap里，已经没有其他节点会和它关联了，所有的connections关系已经迁移到新的角色替代节点上
            if (tri.isDeleted) {
                continue;
            }

            // 更新当前点的重要性（z 轴存储effective area,这是一个单调增的指标）
            if (tri.area > previousEA) {
                previousEA = tri.area;
            }
            results.get(tri.indices[1]).z = previousEA;
            //      System.out.println(tri.indices[1] + "," + previousEA);

//            System.out.println(Arrays.toString(tri.indices) + "," + previousEA);

            // 更新相邻三角形
            if (tri.prev != null) {
                // 标记为失效点，同时new一个新的对象接管它的一切数据和前后连接关系，然后更新前后连接关系、更新significance、加入heap使其排好序

                // 1. 处理旧的tri.prev被标记删除的事情（角色替代）
                // triangleHeap.remove(tri.prev); // Avoid using heap.remove(x) as it is O(n) complexity!
                tri.prev.markDeleted(); // O(1) 这个点标记为废掉，前后关联都砍断，但是不remove因为那太耗时，只要heap poll到它就跳过就可以

                Triangle newPre = new Triangle(tri.prev); // deep copy and inherit connection
//                tri.prev = newPre; // replace TODO can omit

                // 2. 处理pi被淘汰引起tri.prev被更新的事情
                // 前一个三角形连到后一个三角形
                tri.prev.next = tri.next; // ATTENTION!!!: 这里的tri.next后面可能会因为处理旧的tri.next被标记删除的事情被换掉！到时候要重新赋值！
                tri.prev.indices[2] = tri.indices[2];
                tri.prev.area = triArea(lineToSimplify.get(tri.prev.indices[0]), lineToSimplify.get(tri.prev.indices[1]), lineToSimplify.get(tri.prev.indices[2]));

                // 重新加入堆
                // 在 Java 的 PriorityQueue 中，修改元素的属性不会自动更新堆的顺序
                // 所以必须通过add来显式重新插入修改后的元素
                triangleHeap.add(tri.prev); // O(logn) 注意加入的是一个新的对象isDeleted=false
            }

            if (tri.next != null) {
                // 标记为失效点，同时new一个新的对象接管它的一切数据和前后连接关系，然后更新前后连接关系、更新significance、加入heap使其排好序

                // 1. 处理旧的tri.next被标记删除的事情（角色替代）
                // triangleHeap.remove(tri.next); // Avoid using heap.remove(x) as it is O(n) complexity
                tri.next.markDeleted(); // O(1) 这个点标记为废掉，前后关联都砍断，但是不remove因为那太耗时，只有poll到它就跳过就可以

                Triangle newNext = new Triangle(tri.next); // deep copy and inherit connection
//                tri.next = newNext; // replace TODO can omit

                if (tri.prev != null) {
                    tri.prev.next = tri.next; // ATTENTION!!!: 这里的tri.next已经被换掉！所以之前的要重新赋值！
                }

                // 2. 处理pi被淘汰引起tri.next被更新的事情
                tri.next.prev = tri.prev; // 注意此时tri.prev已经是替代后的节点，tri.next也是，从而被标记为废点的前后关联真正砍断
                tri.next.indices[0] = tri.indices[0];
                tri.next.area = triArea(lineToSimplify.get(tri.next.indices[0]), lineToSimplify.get(tri.next.indices[1]), lineToSimplify.get(tri.next.indices[2]));

                // 重新加入堆
                // 在 Java 的 PriorityQueue 中，修改元素的属性不会自动更新堆的顺序
                // 所以必须通过add 来显式重新插入修改后的元素
                triangleHeap.add(tri.next); // 注意加入的是一个新的对象isDeleted=false
            }
        }
    }

    public static void main(String[] args) {
        Polyline polyline = new Polyline();
        List<Polyline> polylineList = new ArrayList<>();
        Random rand = new Random(1);
        int n = 1000_000;

        int p = 1000;
        for (int i = 0; i < n; i += p) {
            Polyline polylineBatch = new Polyline();
            for (int j = i; j < Math.min(i + p, n); j++) {
                double v = rand.nextInt(1000000);

                polyline.addVertex(new vPoint(j, v));

                polylineBatch.addVertex(new vPoint(j, v));
            }
            polylineList.add(polylineBatch);
        }

//        try (FileWriter writer = new FileWriter("raw.csv")) {
//            // 写入CSV头部
//            writer.append("x,y,z\n");
//
//            // 写入每个点的数据
//            for (int i = 0; i < polyline.size(); i++) {
//                vPoint point = polyline.get(i);
//                writer.append(point.x + "," + point.y + "," + point.z + "\n");
//            }
//            System.out.println("Data has been written");
//        } catch (IOException e) {
//            System.out.println("Error writing to CSV file: " + e.getMessage());
//        }

        System.out.println("---------------------------------");
        List<vPoint> results = new ArrayList<>();
        // 计算运行时间
        long startTime = System.currentTimeMillis();
        buildEffectiveArea(polyline, results);
        // 输出结果
        long endTime = System.currentTimeMillis();
        System.out.println("Time taken to reduce points: " + (endTime - startTime) + "ms");
        System.out.println(results.size());

        if (results.size() <= 100) {
            System.out.println("+++++++++++++++++++");
            for (int i = 0; i < results.size(); i++) {
                vPoint point = results.get(i);
                System.out.println("Point: (" + point.x + ", " + point.y + ", " + point.z + ")");
            }
        }

//        try (FileWriter writer = new FileWriter("fast.csv")) {
//            // 写入CSV头部
//            writer.append("x,y,z\n");
//
//            // 写入每个点的数据
//            for (int i = 0; i < results.size(); i++) {
//                vPoint point = results.get(i);
//                writer.append(point.x + "," + point.y + "," + point.z + "\n");
//            }
//            System.out.println("Data has been written");
//        } catch (IOException e) {
//            System.out.println("Error writing to CSV file: " + e.getMessage());
//        }

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
