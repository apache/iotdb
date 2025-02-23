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

public class eBUG {
  public static List<Point> findEliminated(List<Point> points, int pa_idx, int pb_idx) {
    // pa: left adjacent non-eliminated point of pi
    // pb: right adjacent non-eliminated point of pi
    // return a list of points, T'_max{0,k-e}[a:b] order by time in ascending order

    // 性质：最近淘汰的那一个点一定是位于pa~pb之间，因为正是这个点的淘汰才使得pi的significance要更新！至于其他全局更早淘汰的点则不一定位于a:b之间
    // pa,xxx,pi,xxx,pb
    // when e=0，遍历点数就是3
    // when e>0，遍历点数大于等于4、小于等于e+3

    List<Point> res = new ArrayList<>();
    Point start = points.get(pa_idx);
    Point end = points.get(pb_idx);
    //        int cnt = 0;
    while (start
        != end) { // Point类里增加prev&next指针，这样T'_max{0,k-e}里点的连接关系就有了，这样从Pa开始沿着指针，遍历点数一定不超过e+3
      res.add(start);
      start = start.next; // when e=0, only traversing three points pa pi pb
      //            cnt += 1;
    }
    res.add(end);
    //        cnt += 1;
    //        System.out.println(cnt); // 3<=cnt<=e+3

    return res;
  }

  public static List<Point> buildEffectiveArea(Polyline lineToSimplify, int e, boolean debug) {
    // precomputation mode
    return buildEffectiveArea(lineToSimplify.getVertices(), e, debug, 0);
  }

  public static List<Point> buildEffectiveArea(
      Polyline lineToSimplify, int e, boolean debug, int m) {
    // precomputation mode
    return buildEffectiveArea(lineToSimplify.getVertices(), e, debug, m);
  }

  public static List<Point> buildEffectiveArea(List<Point> points, int e, boolean debug) {
    // precomputation mode
    return buildEffectiveArea(points, e, debug, 0);
  }

  public static List<Point> buildEffectiveArea(List<Point> points, int e, boolean debug, int m) {
    if (e < 0) {
      throw new IllegalArgumentException("Parameter e must be non-negative.");
    }

    if (debug) {
      if (m > 2) {
        System.out.println(
            "online sampling mode, "
                + "returning "
                + m
                + " sampled points sorted by time in ascending order");
      } else {
        System.out.println(
            "offline precomputation mode, "
                + "returning each point sorted by dominated significance (DS) in ascending order");
      }
    }

    //        List<Point> results = lineToSimplify.getVertices(); // 浅复制
    // TODO 预计算结果改成按照bottom-up逐点淘汰顺序（DS递增）排列而不是按照时间戳，这样省去在线时对DS排序的过程
    //    add的是Point引用，所以没有多用一倍的空间
    List<Point> resultsBottomUpEliminated = new ArrayList<>();

    // 存储的是点的引用，这样可以修改原来序列里点的淘汰状态
    // 存储的是距离当前最新状态的滞后的尚未施加、待施加的e个淘汰点
    LinkedList<Point> laggedEliminatedPoints = new LinkedList<>();

    int total = points.size();
    if (total < 3) {
      return points; // 不足 3 个点无法形成三角形
    }

    int nTriangles = total - 2;
    Triangle[] triangles = new Triangle[nTriangles];

    // 创建所有三角形并计算初始面积
    points.get(0).prev = null;
    points.get(0).next = points.get(1);
    points.get(total - 1).next = null;
    points.get(total - 1).prev = points.get(total - 2);
    for (int i = 1; i < total - 1; i++) {
      int index1 = i - 1, index2 = i, index3 = i + 1;
      double area = triArea(points.get(index1), points.get(index2), points.get(index3));
      triangles[i - 1] = new Triangle(index1, index2, index3, area);

      // 初始化点的状态 for eBUG usage 用于快速按照按照时间顺序排列的滞后k个淘汰点状态下位于pa~pb的点；也用于最后在线采样结果顺序输出未淘汰点
      points.get(i).prev = points.get(i - 1);
      points.get(i).next = points.get(i + 1);
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
    //        while (!triangleHeap.isEmpty()) { // TODO 注意triangleHeap里装的是non-terminal point对应的三角形
    // TODO 在线采样m个点，也就是说最后留下m-2个non-terminal point
    //      注意：triangleHeap里装的包括了标记删除的点！所以triangleHeap.size()不是真正的留下的未被淘汰点数！
    //      因此需要用fakeCnt来统计heap里的非真实点数，这样triangleHeap.size()-fakeCnt就是真正的留下的未被淘汰点数
    int remainNonTerminalPoint = Math.max(0, m - 2);
    int fakeCnt = 0;
    while (triangleHeap.size() - fakeCnt > remainNonTerminalPoint) {
      // 注意peek只需要直接访问该位置的元素，不涉及任何重排或堆化操作
      // 而poll是删除堆顶元素，需要重新堆化以维护堆的性质，复杂度是O(logk),k是当前堆的大小
      Triangle tri = triangleHeap.poll(); // O(logn)

      // 如果该三角形已经被删除，跳过. Avoid using heap.remove(x) as it is O(n) complexity
      // 而且除了heap里，已经没有其他节点会和它关联了，所有的connections关系已经迁移到新的角色替代节点上
      if (tri.isDeleted) {
        fakeCnt--; // 取出了一个被标记删除点
        if (debug) {
          System.out.println(
              ">>>bottom-up, remaining "
                  + triangleHeap.size()
                  + " triangles (including those marked for removal)");
        }
        continue;
      }

      // 真正的淘汰点
      // 记录最近淘汰的点，注意不要重复记录也就是在上面执行之后再确认淘汰
      if (debug) {
        System.out.println("(1) eliminate " + points.get(tri.indices[1]));
      }
      if (e > 0) {
        if (laggedEliminatedPoints.size() == e) {
          // 已经有e个滞后淘汰点了，为了把最近淘汰点加入，得先把最老的淘汰点施加上去
          Point removedPoint = laggedEliminatedPoints.removeFirst(); // 取出最早加入的淘汰点 复杂度1
          removedPoint.markEliminated(); // 注意是引用，所以改的内容后面可以看到
        }
        laggedEliminatedPoints.addLast(points.get(tri.indices[1])); // 加入最新的淘汰点，滞后在这里先不施加
      } else { // e=0 没有滞后 立即标记删除 T'_k
        points.get(tri.indices[1]).markEliminated();
      }
      if (debug) {
        System.out.println(
            "the e most recently eliminated points (lagged):" + laggedEliminatedPoints);
      }

      // 更新当前点的重要性（z 轴存储effective area,这是一个单调增的指标）
      if (m <= 2) { // precomputation mode
        if (tri.area > previousEA) {
          previousEA = tri.area;
        }
        //            results.get(tri.indices[1]).z = previousEA; // dominated significance
        points.get(tri.indices[1]).z = previousEA; // dominated significance
        resultsBottomUpEliminated.add(points.get(tri.indices[1])); // TODO add的是Point引用，所以没有多用一倍的空间
        if (debug) {
          System.out.println(Arrays.toString(tri.indices) + ", Dominated Sig=" + previousEA);
        }
      }

      // 更新相邻三角形
      if (tri.prev != null) {
        // 标记为失效点，同时new一个新的对象接管它的一切数据和前后连接关系，然后更新前后连接关系、更新significance、加入heap使其排好序

        // 1. 处理旧的tri.prev被标记删除的事情（角色替代）
        // triangleHeap.remove(tri.prev); // Avoid using heap.remove(x) as it is O(n) complexity!
        tri.prev.markDeleted(); // O(1) 这个点标记为废掉，前后关联都砍断，但是不remove因为那太耗时，只要heap poll到它就跳过就可以

        Triangle newPre = new Triangle(tri.prev); // deep copy and inherit connection
        // tri.prev = newPre; // can omit, because already done by new Triangle(tri.prev)

        // 2. 处理pi被淘汰引起tri.prev被更新的事情
        // 前一个三角形连到后一个三角形
        tri.prev.next =
            tri.next; // ATTENTION!!!: 这里的tri.next后面可能会因为处理旧的tri.next被标记删除的事情被换掉！到时候要重新赋值！
        tri.prev.indices[2] = tri.indices[2];

        // e parameter
        double sig;
        if (e > 0) {
          List<Point> pointsForSig =
              findEliminated(points, tri.prev.indices[0], tri.prev.indices[2]);
          if (debug) {
            System.out.println("(2) update point on the left " + points.get(tri.prev.indices[1]));
            System.out.println("3<=cnt<=e+3. cnt=" + pointsForSig.size());
            for (Point point : pointsForSig) {
              System.out.println("\t" + point);
            }
          }
          List<Point> baseLine = new ArrayList<>();
          baseLine.add(pointsForSig.get(0));
          baseLine.add(pointsForSig.get(pointsForSig.size() - 1)); // 直接连接左右两边最近的未被淘汰的点
          sig = total_areal_displacement(pointsForSig, baseLine, false);
        } else { // 直接用三角形面积
          sig =
              triArea(
                  points.get(tri.prev.indices[0]),
                  points.get(tri.prev.indices[1]),
                  points.get(tri.prev.indices[2]));
        }
        tri.prev.area = sig;

        if (debug) {
          System.out.println("sig=" + sig);
          double tmpTri =
              triArea(
                  points.get(tri.prev.indices[0]),
                  points.get(tri.prev.indices[1]),
                  points.get(tri.prev.indices[2]));
          System.out.println(
              "\t"
                  + "tri="
                  + tmpTri
                  + ", "
                  + ((tmpTri > sig) ? "over-estimated" : "equal/less-estimated"));
        }

        // 重新加入堆
        // 在 Java 的 PriorityQueue 中，修改元素的属性不会自动更新堆的顺序
        // 所以必须通过add来显式重新插入修改后的元素
        triangleHeap.add(tri.prev); // O(logn) 注意加入的是一个新的对象isDeleted=false
        fakeCnt++; // 表示heap里多了一个被标记删除的假点
      }

      if (tri.next != null) {
        // 标记为失效点，同时new一个新的对象接管它的一切数据和前后连接关系，然后更新前后连接关系、更新significance、加入heap使其排好序

        // 1. 处理旧的tri.next被标记删除的事情（角色替代）
        // triangleHeap.remove(tri.next); // Avoid using heap.remove(x) as it is O(n) complexity
        tri.next.markDeleted(); // O(1) 这个点标记为废掉，前后关联都砍断，但是不remove因为那太耗时，只有poll到它就跳过就可以

        Triangle newNext = new Triangle(tri.next); // deep copy and inherit connection
        // tri.next = newNext; // omit, because already done by new Triangle(tri.prev)

        if (tri.prev != null) {
          tri.prev.next = tri.next; // ATTENTION!!!: 这里的tri.next已经被换掉！所以之前的要重新赋值！
        }

        // 2. 处理pi被淘汰引起tri.next被更新的事情
        tri.next.prev = tri.prev; // 注意此时tri.prev已经是替代后的节点，tri.next也是，从而被标记为废点的前后关联真正砍断
        tri.next.indices[0] = tri.indices[0];

        // e parameter
        double sig;
        if (e > 0) {
          List<Point> pointsForSig =
              findEliminated(points, tri.next.indices[0], tri.next.indices[2]);
          if (debug) {
            System.out.println(
                "(2) updating point on the right " + points.get(tri.next.indices[1]));
            System.out.println("3<=cnt<=e+3. cnt=" + pointsForSig.size());
            for (Point point : pointsForSig) {
              System.out.println("\t" + point);
            }
          }
          List<Point> baseLine = new ArrayList<>();
          baseLine.add(pointsForSig.get(0));
          baseLine.add(pointsForSig.get(pointsForSig.size() - 1)); // 直接连接左右两边最近的未被淘汰的点
          sig = total_areal_displacement(pointsForSig, baseLine, false);
        } else { // 直接用三角形面积
          sig =
              triArea(
                  points.get(tri.next.indices[0]),
                  points.get(tri.next.indices[1]),
                  points.get(tri.next.indices[2]));
        }
        tri.next.area = sig;

        if (debug) {
          System.out.println("sig=" + sig);
          double tmpTri =
              triArea(
                  points.get(tri.next.indices[0]),
                  points.get(tri.next.indices[1]),
                  points.get(tri.next.indices[2]));
          System.out.println(
              "\t"
                  + "tri="
                  + tmpTri
                  + ", "
                  + ((tmpTri > sig) ? "over-estimated" : "equal/less-estimated"));
        }

        // 重新加入堆
        // 在 Java 的 PriorityQueue 中，修改元素的属性不会自动更新堆的顺序
        // 所以必须通过add 来显式重新插入修改后的元素
        triangleHeap.add(tri.next); // 注意加入的是一个新的对象isDeleted=false
        fakeCnt++; // 表示heap里多了一个被标记删除的假点
      }
      if (debug) {
        System.out.println(
            ">>>bottom-up, remaining "
                + triangleHeap.size()
                + " triangles (including those marked for removal)");
      }
    }

    if (m > 2) { // online sampling mode
      // 把滞后的淘汰点施加上去，然后返回在线采样结果（也就是返回剩余未被淘汰的点）
      // 注意未淘汰的点的Dominated significance尚未赋值，还都是infinity
      // 不过既然m>2在线采样模式，所以其实淘汰点的DS本身也没有记录
      for (Point p : laggedEliminatedPoints) {
        p.markEliminated();
        if (debug) {
          System.out.println("apply lagged elimination of " + p);
        }
      }
      List<Point> onlineSampled = new ArrayList<>();
      Point start = points.get(0);
      Point end = points.get(points.size() - 1);
      while (start
          != end) { // Point类里增加prev&next指针，这样T'_max{0,k-e}里点的连接关系就有了，这样从Pa开始沿着指针，遍历点数一定不超过e+3
        onlineSampled.add(start);
        start = start.next; // when e=0, only traversing three points pa pi pb
      }
      onlineSampled.add(end);
      return onlineSampled;
    } else { // offline precomputation mode, for precomputing the dominated significance of each
      // point
      //            return results; // 注意这就是lineToSimplify.getVertices()
      resultsBottomUpEliminated.add(points.get(0)); // 全局首点
      resultsBottomUpEliminated.add(points.get(points.size() - 1)); // 全局尾点

      // make them ordered by dominated sig in descending order (i.e., reverse of bottom-up
      // elimination order)
      Collections.reverse(resultsBottomUpEliminated); // O(n)

      return resultsBottomUpEliminated;
    }
  }

  public static void main(String[] args) {
    // 实验：预计算耗时关于两个参数e、n的变化规律，是否符合复杂度理论建模

    //        int eParam = 0;
    //        int[] eParamList = {0, 0, 1, 2, 3, 4, 5, 10, 14, 15, 16, 20, 30, 40, 50, 100, 200,
    // 500, 1000, 2000, 5000,
    //                10000, 50000, 10_0000, 50_0000, 100_0000, 200_0000, 300_0000,
    //                500_0000, 800_0000, 1000_0000,
    //                1500_0000, 2000_0000, 2500_0000, 3000_0000};
    int[] eParamList = {100_0000};
    for (int eParam : eParamList) {
      try (PrintWriter writer = new PrintWriter(new File("exp_varyN_e" + eParam + ".csv"))) {
        for (int n = 100_0000; n < 2000_0000; n += 200_0000) {
          // TODO 注意 要有一个点是n=300w

          Polyline polyline = new Polyline();
          int seed = 1;
          Random rand = new Random(seed); // TODO 注意seed在每轮里重设
          for (int i = 0; i < n; i++) {
            double v = rand.nextInt(1000000);
            polyline.addVertex(new Point(i, v));
          }

          long startTime = System.currentTimeMillis();
          List<Point> results = buildEffectiveArea(polyline, eParam, false, 0);
          long endTime = System.currentTimeMillis();

          System.out.println(
              "n="
                  + n
                  + ", e="
                  + eParam
                  + ", Time taken to reduce points: "
                  + (endTime - startTime)
                  + "ms");
          writer.println(n + "," + eParam + "," + (endTime - startTime));
          System.out.println(results.size());

          //                // clear
          //                polyline.clear();
          //                results.clear();
          //                polyline = null;
          //                results = null;
          //                System.gc();
        }
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    //        int n = 300_0000;
    //
    //        int seed = 1;
    //        Random rand = new Random(seed); // TODO 注意seed在每轮里重设
    //        Polyline polyline = new Polyline(); // 注意：如果是固定n变化e实验，这个数据要一次性生成，不然每次随机不一样
    //        for (int i = 0; i < n; i++) {
    //            double v = rand.nextInt(1000000);
    //            polyline.addVertex(new Point(i, v)); //
    // point的状态会在buildEffectiveArea里重置的，所以在下面的e遍历里可以复用这一份数据
    //        }
    //
    //        try (PrintWriter writer = new PrintWriter(new File("exp_varyE_n" + n + ".csv"))) {
    //            int[] eParamList = {0, 1, 2, 10, 1000, 10000, 10_0000, 30_0000, 50_0000, 60_0000,
    // 80_0000, 100_0000, 150_0000, 200_0000};
    //            for (int eParam : eParamList) {
    //                eParam *= 3; // TODO 注意 因为n=300w而不是100w
    //
    //                long startTime = System.currentTimeMillis();
    //                List<Point> results = buildEffectiveArea(polyline, eParam, false);
    //                long endTime = System.currentTimeMillis();
    //
    //                System.out.println("n=" + n + ", e=" + eParam + ", Time taken to reduce
    // points: " + (endTime - startTime) + "ms");
    //                writer.println(n + "," + eParam + "," + (endTime - startTime));
    //                System.out.println(results.size());
    //
    //                // clear 注意不能把原始数据清除，还要接着用
    ////                System.gc();
    //            }
    //        } catch (FileNotFoundException e) {
    //            throw new RuntimeException(e);
    //        }

    System.out.println("finish");
  }
}
