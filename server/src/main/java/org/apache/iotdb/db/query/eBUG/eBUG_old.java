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

package org.apache.iotdb.db.query.eBUG;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

import static org.apache.iotdb.db.query.eBUG.Tool.total_areal_displacement;
import static org.apache.iotdb.db.query.eBUG.Tool.triArea;


public class eBUG_old {
    public static List<Point> findEliminated(Polyline lineToSimplify, int[] recentEliminated,
                                             int pa_idx, int pi_idx, int pb_idx) {
        // TODO 复杂度分析
        //  从最近淘汰的e个点里寻找位于pa~pb之间的：e
        //  把上一步找到的点和pa pi pb一起按照时间戳递增排序：记c=b-a。如果e<c，那么eloge，否则c
        //  后面鞋带公式计算面积：min(e+3,c)
        // 性质：最近淘汰的那一个点一定是位于pa~pb之间，因为正是这个点的淘汰才使得pi的significance要更新！
        // pi: the point whose significance needs updating
        // pa: left adjacent non-eliminated point of pi
        // pb: right adjacent non-eliminated point of pi
        // return a list of points, containing pa,p,pb and points between pa&pa from the e most recently eliminated points,
        // order by time in ascending order

        Point pa = lineToSimplify.get(pa_idx);
        Point pi = lineToSimplify.get(pi_idx);
        Point pb = lineToSimplify.get(pb_idx);

        List<Point> res = new ArrayList<>();

//        if (recentEliminated.length == 0) { // e=0
//            System.out.println("e=0");
//            res.add(pa);
//            res.add(pi);
//            res.add(pb);
//            return res; // pa pi pb已经按照时间戳递增
//        }

        if (recentEliminated.length >= (pb_idx - pa_idx - 2)) { // e >= the total number of eliminated points in this region
//            System.out.println("[c=(b-a-2)]<=e, use T directly"); // worst case下, k=c
            res = lineToSimplify.getVertices().subList(pa_idx, pb_idx + 1); // 左闭右开
            return res; // 已经按照时间戳递增
        }

        // 从最近淘汰的e个点里寻找位于pa~pb之间的点，找到的有可能小于e个点
//        System.out.println("k>=[c=(b-a-2)]>e, search among e");
        res.add(pa);
        res.add(pi);
        res.add(pb);

        // 包括了c>e=0的情况
        for (int idx : recentEliminated) { // e points
            if (idx == 0) { // init, not filled by real eliminated points yet
                break;
            }
            Point p = lineToSimplify.get(idx);
            if (p.x > pa.x && p.x < pb.x) {
                res.add(p);
            }
        }

        // 按照时间戳递增排序，这是为了后面计算total areal displacement需要
        res.sort(Comparator.comparingDouble(p -> p.x)); // 按时间戳排序 TODO 这样的话时间复杂度要变成e*log(e)了吧？

        return res;
    }

    public static List<Point> buildEffectiveArea(Polyline lineToSimplify, int e, boolean debug) {
        if (e < 0) {
            throw new IllegalArgumentException("Parameter e must be non-negative.");
        }

        List<Point> results = lineToSimplify.getVertices(); // 浅复制
//        results.addAll(lineToSimplify.getVertices()); // TODO debug

        // TODO 需要一个东西来记录最近淘汰的点依次是谁
        int[] recentEliminated = new int[e]; // init 0 points to the first point, skipped in findEliminated
        int recentEliminatedIdx = 0;  // index in the array, circulate

        int total = lineToSimplify.size();
        if (total < 3) {
            return results; // 不足 3 个点无法形成三角形
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
        Collections.addAll(triangleHeap, triangles); // complexity TODO O(n) or O(nlogn)?

        double previousEA = -1;
        while (!triangleHeap.isEmpty()) {
            // 注意peek只需要直接访问该位置的元素，不涉及任何重排或堆化操作
            // 而poll是删除堆顶元素，需要重新堆化以维护堆的性质，复杂度是O(logk),k是当前堆的大小
            Triangle tri = triangleHeap.poll(); // O(logn)

            // 如果该三角形已经被删除，跳过. Avoid using heap.remove(x) as it is O(n) complexity
            // 而且除了heap里，已经没有其他节点会和它关联了，所有的connections关系已经迁移到新的角色替代节点上
            if (tri.isDeleted) {
                if (debug) {
                    System.out.println(">>>bottom-up, remaining " + triangleHeap.size() + " triangles (including those marked for removal)");
                }
                continue;
            }

            // 真正的淘汰点
            // 记录最近淘汰的点，注意不要重复记录也就是在上面执行之后再确认淘汰
            if (debug) {
                System.out.println("(1) eliminate " + lineToSimplify.get(tri.indices[1]));
            }
            if (e > 0) { // otherwise e=0
                recentEliminated[recentEliminatedIdx] = tri.indices[1];
                recentEliminatedIdx = (recentEliminatedIdx + 1) % e; // 0~e-1 circulate
            }
            if (debug) {
                System.out.println("the e most recently eliminated points:" + Arrays.toString(recentEliminated));
            }

            // 更新当前点的重要性（z 轴存储effective area,这是一个单调增的指标）
            if (tri.area > previousEA) {
                previousEA = tri.area;
            }
            results.get(tri.indices[1]).z = previousEA; // dominated significance
            if (debug) {
                System.out.println(Arrays.toString(tri.indices) + ", Dominated Sig=" + previousEA);
            }

            // 更新相邻三角形
            if (tri.prev != null) {
                // 标记为失效点，同时new一个新的对象接管它的一切数据和前后连接关系，然后更新前后连接关系、更新significance、加入heap使其排好序

                // 1. 处理旧的tri.prev被标记删除的事情（角色替代）
                // triangleHeap.remove(tri.prev); // Avoid using heap.remove(x) as it is O(n) complexity!
                tri.prev.markDeleted(); // O(1) 这个点标记为废掉，前后关联都砍断，但是不remove因为那太耗时，只要heap poll到它就跳过就可以

                Triangle newPre = new Triangle(tri.prev); // deep copy and inherit connection
//                tri.prev = newPre; // can omit, because already done by new Triangle(tri.prev)

                // 2. 处理pi被淘汰引起tri.prev被更新的事情
                // 前一个三角形连到后一个三角形
                tri.prev.next = tri.next; // ATTENTION!!!: 这里的tri.next后面可能会因为处理旧的tri.next被标记删除的事情被换掉！到时候要重新赋值！
                tri.prev.indices[2] = tri.indices[2];

                // e parameter
                List<Point> pointsForSig = findEliminated(lineToSimplify, recentEliminated,
                        tri.prev.indices[0],
                        tri.prev.indices[1],
                        tri.prev.indices[2]); // TODO complexity ?
                if (debug) {
                    System.out.println("(2) update point on the left " + lineToSimplify.get(tri.prev.indices[1]));
                    for (Point point : pointsForSig) {
                        System.out.println("\t" + point);
                    }
                }
                List<Point> baseLine = new ArrayList<>();
                baseLine.add(pointsForSig.get(0));
                baseLine.add(pointsForSig.get(pointsForSig.size() - 1)); // 直接连接左右两边最近的未被淘汰的点
                double sig = total_areal_displacement(pointsForSig, baseLine, false);
                tri.prev.area = sig;

                if (debug) {
                    System.out.println("sig=" + sig);
                    double tmpTri = triArea(lineToSimplify.get(tri.prev.indices[0]),
                            lineToSimplify.get(tri.prev.indices[1]),
                            lineToSimplify.get(tri.prev.indices[2]));
                    System.out.println("\t" + "tri=" + tmpTri + ", " + ((tmpTri > sig) ? "over-estimated" : "equal/less-estimated"));
                }

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
//                tri.next = newNext; // omit, because already done by new Triangle(tri.prev)

                if (tri.prev != null) {
                    tri.prev.next = tri.next; // ATTENTION!!!: 这里的tri.next已经被换掉！所以之前的要重新赋值！
                }

                // 2. 处理pi被淘汰引起tri.next被更新的事情
                tri.next.prev = tri.prev; // 注意此时tri.prev已经是替代后的节点，tri.next也是，从而被标记为废点的前后关联真正砍断
                tri.next.indices[0] = tri.indices[0];

                // e parameter
                List<Point> pointsForSig = findEliminated(lineToSimplify, recentEliminated,
                        tri.next.indices[0],
                        tri.next.indices[1],
                        tri.next.indices[2]); // TODO complexity
                if (debug) {
                    System.out.println("(2) updating point on the right " + lineToSimplify.get(tri.next.indices[1]));
                    for (Point point : pointsForSig) {
                        System.out.println("\t" + point);
                    }
                }
                List<Point> baseLine = new ArrayList<>();
                baseLine.add(pointsForSig.get(0));
                baseLine.add(pointsForSig.get(pointsForSig.size() - 1)); // 直接连接左右两边最近的未被淘汰的点
                double sig = total_areal_displacement(pointsForSig, baseLine, false);
                tri.next.area = sig;

                if (debug) {
                    System.out.println("sig=" + sig);
                    double tmpTri = triArea(lineToSimplify.get(tri.next.indices[0]),
                            lineToSimplify.get(tri.next.indices[1]),
                            lineToSimplify.get(tri.next.indices[2]));
                    System.out.println("\t" + "tri=" + tmpTri + ", " + ((tmpTri > sig) ? "over-estimated" : "equal/less-estimated"));
                }

                // 重新加入堆
                // 在 Java 的 PriorityQueue 中，修改元素的属性不会自动更新堆的顺序
                // 所以必须通过add 来显式重新插入修改后的元素
                triangleHeap.add(tri.next); // 注意加入的是一个新的对象isDeleted=false
            }
            if (debug) {
                System.out.println(">>>bottom-up, remaining " + triangleHeap.size() + " triangles (including those marked for removal)");
            }
        }
        return results; // 注意这就是lineToSimplify.getVertices()
    }

    public static void main(String[] args) {
        Polyline polyline = new Polyline();
        List<Polyline> polylineList = new ArrayList<>();
        Random rand = new Random(1);
        int n = 100_0000;

        int p = 10;
        for (int i = 0; i < n; i += p) {
            Polyline polylineBatch = new Polyline();
            for (int j = i; j < Math.min(i + p, n); j++) {
                double v = rand.nextInt(1000000);

                polyline.addVertex(new Point(j, v));

                polylineBatch.addVertex(new Point(j, v));
            }
            polylineList.add(polylineBatch);
        }

//        try (FileWriter writer = new FileWriter("raw.csv")) {
//            // 写入CSV头部
//            writer.append("x,y,z\n");
//
//            // 写入每个点的数据
//            for (int i = 0; i < polyline.size(); i++) {
//                Point point = polyline.get(i);
//                writer.append(point.x + "," + point.y + "," + point.z + "\n");
//            }
//            System.out.println("Data has been written");
//        } catch (IOException e) {
//            System.out.println("Error writing to CSV file: " + e.getMessage());
//        }

        System.out.println("---------------------------------");
//        List<Point> results = new ArrayList<>();
        // 计算运行时间
//        int eParam = 10;
        try (PrintWriter writer = new PrintWriter(new File("exp.csv"))) {
            int[] eParamList = {0, 1, 100, 500, 1000, 5000, 10000, 50000, 10_0000, 30_0000, 50_0000, 60_0000,
                    70_0000, 80_0000, 90_0000, 100_0000, 150_0000, 200_0000, 250_0000, 300_0000};
//            for (int eParam = 0; eParam < 2 * n; eParam += 1000) {
            for (int eParam : eParamList) {
                long startTime = System.currentTimeMillis();
                List<Point> results = buildEffectiveArea(polyline, eParam, false);
                // 输出结果
                long endTime = System.currentTimeMillis();
                System.out.println("n=" + n + ", e=" + eParam + ", Time taken to reduce points: " + (endTime - startTime) + "ms");
                System.out.println(results.size());
                writer.println(n + "," + eParam + "," + (endTime - startTime));
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

//        if (results.size() <= 100) {
//            System.out.println("+++++++++++++++++++");
//            for (int i = 0; i < results.size(); i++) {
//                Point point = results.get(i);
//                System.out.println("Point: (" + point.x + ", " + point.y + ", " + point.z + ")");
//            }
//        }

//        try (FileWriter writer = new FileWriter("fast.csv")) {
//            // 写入CSV头部
//            writer.append("x,y,z\n");
//
//            // 写入每个点的数据
//            for (int i = 0; i < results.size(); i++) {
//                Point point = results.get(i);
//                writer.append(point.x + "," + point.y + "," + point.z + "\n");
//            }
//            System.out.println("Data has been written");
//        } catch (IOException e) {
//            System.out.println("Error writing to CSV file: " + e.getMessage());
//        }

//        System.out.println("---------------------------------");
//        List<List<Point>> resultsBatchList = new ArrayList<>();
//        // 计算运行时间
//        int cnt = 0;
//        startTime = System.currentTimeMillis();
//        for (Polyline polylineBatch : polylineList) {
//            List<Point> resultsBatch = new ArrayList<>();
//            buildEffectiveArea(polylineBatch, resultsBatch);
//            cnt += resultsBatch.size();
//            resultsBatchList.add(resultsBatch);
//        }
//        // 输出结果
//        endTime = System.currentTimeMillis();
//        System.out.println("Time taken to reduce points: " + (endTime - startTime) + "ms");
//        System.out.println(cnt);
//
//        System.out.println("---------------------------------");
//        // 使用 Stream API 合并所有列表
//        List<Point> mergedList = resultsBatchList.stream().flatMap(List::stream).collect(Collectors.toList());
//        int sameCnt = 0;
//        for (int i = 0; i < mergedList.size(); i++) {
//            if (mergedList.get(i).z == results.get(i).z) {
//                sameCnt += 1;
//            }
//        }
//        System.out.println("sameCnt=" + sameCnt + ", percent=" + sameCnt * 1.0 / mergedList.size());
//
//        try (FileWriter writer = new FileWriter("batch.csv")) {
//            // 写入CSV头部
//            writer.append("x,y,z\n");
//
//            // 写入每个点的数据
//            for (int i = 0; i < mergedList.size(); i++) {
//                Point point = mergedList.get(i);
//                writer.append(point.x + "," + point.y + "," + point.z + "\n");
//            }
//            System.out.println("Data has been written");
//        } catch (IOException e) {
//            System.out.println("Error writing to CSV file: " + e.getMessage());
//        }
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
