package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.*;

public class SWAB_AD {
  public static List<Point> seg_bottomUp_maxerror_withTimestamps(
      List<Point> points, double maxError, Object[] prefixSum, boolean debug) {
    int total = points.size();
    if (total < 3) {
      return points; // 不足 3 个点无法形成三角形
    }

    // 每个triangle对应两个相邻的待合并的分段
    int nTriangles = total - 2;
    Triangle[] triangles = new Triangle[nTriangles];

    // 创建所有三角形并计算初始面积
    points.get(0).prev = null;
    points.get(0).next = points.get(1);
    //        points.get(0).index = 0;
    points.get(total - 1).next = null;
    points.get(total - 1).prev = points.get(total - 2);
    //        points.get(total - 1).index = points.size() - 1;
    for (int i = 1; i < total - 1; i++) {
      int index1 = i - 1, index2 = i, index3 = i + 1;
      // 相当于e=infinity
      double mc = DP.joint_segment_error(points, index1, index3, DP.ERRORtype.area);
      triangles[i - 1] = new Triangle(index1, index2, index3, mc);

      // 初始化点的状态 用于最后在线采样结果顺序输出未淘汰点
      points.get(i).prev = points.get(i - 1);
      points.get(i).next = points.get(i + 1);
      //            points.get(i).index = i; // for swab usage
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

    int fakeCnt = 0;
    while (!triangleHeap.isEmpty() && triangleHeap.peek().area < maxError) {
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
      points.get(tri.indices[1]).markEliminated();

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

        double mc =
            DP.joint_segment_error(
                points, tri.prev.indices[0], tri.prev.indices[2], DP.ERRORtype.area);
        tri.prev.area = mc;
        if (debug) {
          System.out.println(
              "(2) updating point on the left "
                  + points.get(tri.prev.indices[1])
                  + ", ranging ["
                  + tri.prev.indices[0]
                  + ","
                  + tri.prev.indices[2]
                  + "]");
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

        double mc =
            DP.joint_segment_error(
                points, tri.next.indices[0], tri.next.indices[2], DP.ERRORtype.area);
        tri.next.area = mc;
        if (debug) {
          System.out.println(
              "(3) updating point on the right "
                  + points.get(tri.next.indices[1])
                  + ", ranging ["
                  + tri.next.indices[0]
                  + ","
                  + tri.next.indices[2]
                  + "]");
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

    List<Point> onlineSampled = new ArrayList<>();
    Point start = points.get(0);
    Point end = points.get(points.size() - 1);
    while (start
        != end) { // Point类里增加prev&next指针，这样T'_max{0,k-e}里点的连接关系就有了，这样从Pa开始沿着指针，遍历点数一定不超过e+3
      onlineSampled.add(start); // 注意未淘汰的点的Dominated significance尚未赋值，还都是infinity
      start = start.next; // when e=0, only traversing three points pa pi pb
    }
    onlineSampled.add(end);
    return onlineSampled;
  }

  public static int nextSlidingWindowWithTimestamps(
      List<Point> points, double maxError, Object[] prefixSum) {
    if (points.size() <= 1) {
      return points.size() - 1;
    }
    int i = 2; // 从第二个点开始
    while (i < points.size()
        && DP.joint_segment_error(points, 0, i, DP.ERRORtype.area) < maxError) {
      i++; // 窗口扩大
    }
    return i - 1; // 从0开始，并且是从0到i-1的左闭右闭
  }

  public static List<Point> swab_framework(List<Point> points, double maxError, int m) {
    return swab_framework(points, maxError, m, null, false);
  }

  public static List<Point> reducePoints(List<Point> points, double maxError, Object... kwargs)
      throws IOException {
    int m = -1; // 默认值
    for (Object arg : kwargs) {
      if (arg instanceof Integer) {
        m = (Integer) arg; // 获取 m 参数
      }
    }
    if (m <= 0) {
      throw new IOException("please set m>0");
    }
    return swab_framework(points, maxError, m, null, false);
  }

  public static List<Point> swab_framework(
      List<Point> points, double maxError, int m, Object[] prefixSum, boolean debug) {
    if (debug) {
      if (prefixSum != null) {
        // deprecated
        System.out.println("enable prefix sum for accelerating L2 error computation");
      }
    }

    // 在这里给Point添加全局排序idx, 用于全局定位
    for (int i = 0; i < points.size(); i++) {
      points.get(i).index = i;
    }

    if (debug) {
      System.out.println("data length=" + points.size() + ", max_error=" + maxError + ", m=" + m);
    }
    // m在这里的含义是分段数，虽然SWAB不能直接控制分段数，但是也需要这个参数来确定buffer大小
    int bs = (int) Math.floor((double) points.size() / m * 6); // buffer size
    int lb = bs / 2, ub = bs * 2;
    if (lb == 0) {
      lb += 2; // lb最小为2，否则陷入循环，因为joint buffer里至少会留一个点
      ub += 2;
    }
    if (debug) {
      System.out.println("bs=" + bs + ", lb=" + lb + ", ub=" + ub);
    }

    int buffer_startIdx = 0; // 左闭
    int buffer_endIdx = bs; // 右开
    // 注意虽然joint，但是buffer和remaining points是buffer_endIdx处是不重叠的
    // buffer: [0:bs) 左闭右开
    // remaining: [bs:] 左闭

    List<Point> segTs = new ArrayList<>();
    segTs.add(points.get(0)); // 全局首点

    boolean needBottomUp = true; // false when only output, no input
    int reuseIdx = 0; // 注意这里当needBottomUp=false的时候是直接取用上一轮的bottomup points但是base从reuseIdx开始
    List<Point> sampled = Collections.emptyList();
    while (true) {
      if (debug) {
        System.out.println("#####################################################");
        System.out.println(
            "buffer data: ["
                + buffer_startIdx
                + ","
                + buffer_endIdx
                + ")"
                + ", remaining data: ["
                + buffer_endIdx
                + ","
                + points.size()
                + ")");
        System.out.println("reBottomUp:" + needBottomUp);
      }

      if (needBottomUp) {
        if (debug) {
          System.out.println(">>>>bottom up on the buffer");
        }
        reuseIdx = 0; // 重置
        if (prefixSum == null) {
          sampled =
              seg_bottomUp_maxerror_withTimestamps(
                  points.subList(buffer_startIdx, buffer_endIdx), maxError, null, false);
        } else {
          // deprecated
          double[] prefixSumX =
              Arrays.copyOfRange((double[]) prefixSum[0], buffer_startIdx, buffer_endIdx);
          double[] prefixSumY =
              Arrays.copyOfRange((double[]) prefixSum[1], buffer_startIdx, buffer_endIdx);
          double[] prefixSumX2 =
              Arrays.copyOfRange((double[]) prefixSum[2], buffer_startIdx, buffer_endIdx);
          double[] prefixSumY2 =
              Arrays.copyOfRange((double[]) prefixSum[3], buffer_startIdx, buffer_endIdx);
          double[] prefixSumXY =
              Arrays.copyOfRange((double[]) prefixSum[4], buffer_startIdx, buffer_endIdx);
          if (buffer_startIdx > 0) {
            for (int i = 0; i < prefixSumX.length; i++) {
              prefixSumX[i] -= ((double[]) prefixSum[0])[buffer_startIdx - 1];
            }
            for (int i = 0; i < prefixSumY.length; i++) {
              prefixSumY[i] -= ((double[]) prefixSum[1])[buffer_startIdx - 1];
            }
            for (int i = 0; i < prefixSumX2.length; i++) {
              prefixSumX2[i] -= ((double[]) prefixSum[2])[buffer_startIdx - 1];
            }
            for (int i = 0; i < prefixSumY2.length; i++) {
              prefixSumY2[i] -= ((double[]) prefixSum[3])[buffer_startIdx - 1];
            }
            for (int i = 0; i < prefixSumXY.length; i++) {
              prefixSumXY[i] -= ((double[]) prefixSum[4])[buffer_startIdx - 1];
            }
          }
          Object[] prefixSumPart =
              new Object[] {
                prefixSumX,
                prefixSumY,
                prefixSumX2,
                prefixSumY2,
                prefixSumXY,
                prefixSum[5],
                prefixSum[6]
              };
          sampled =
              seg_bottomUp_maxerror_withTimestamps(
                  points.subList(buffer_startIdx, buffer_endIdx), maxError, prefixSumPart, false);
          // TODO prefixsum加速 但是由于精度问题，再加上maxError控制，结果有时候不会完全相同
        }

        // TODO eBUG also
        //                sampled = eBUG.buildEffectiveArea(points.subList(buffer_startIdx,
        // buffer_endIdx), 0, false, m);
      }

      if (debug) {
        System.out.println("number of bottomUp points=" + (sampled.size() - reuseIdx));
        System.out.println("---------------------------------");
      }

      if (sampled.size() - reuseIdx >= 2) { // 左边输出一个segment
        if (debug) {
          System.out.println(
              ">>>>left output a segment:["
                  + sampled.get(reuseIdx).index
                  + ","
                  + sampled.get(1 + reuseIdx).index
                  + "]");
        }
        segTs.add(sampled.get(1 + reuseIdx)); // 分段右端点
        buffer_startIdx = sampled.get(1 + reuseIdx).index; // buffer起点右移
      }

      if (debug) {
        System.out.println(
            "buffer data: ["
                + buffer_startIdx
                + ","
                + buffer_endIdx
                + ")"
                + ", remaining data: ["
                + buffer_endIdx
                + ","
                + points.size()
                + ")");
        System.out.println("---------------------------------");
      }

      if (buffer_endIdx >= points.size()) { // no more point added into buffer w
        if (debug) {
          System.out.println(">>>>no more data remaining");
        }
        // Add remaining segments and break if no more input data
        if (sampled.size() - reuseIdx
            > 2) { // more than one segment, 其中第一段segment已经在上面被记录了，但是没有被移出去
          if (debug) {
            System.out.println(">>>>so just left output all remaining segments");
          }
          for (int k = 2 + reuseIdx; k < sampled.size(); k++) { // 全局尾点就在这里输出
            segTs.add(sampled.get(k));
            if (debug) {
              System.out.println("output: " + sampled.get(k));
            }
          }
        }
        break;
      }

      if ((buffer_endIdx - buffer_startIdx) >= lb) {
        if (debug) {
          System.out.println(
              ">>>>no need adding the sliding segment from right because len(w) >= lb");
        }
        needBottomUp = false;

        reuseIdx++; // 这样简单，不需要sampled.remove(0)复杂度
        if (debug) {
          System.out.println(
              "next iteration uses the remaining segment without re-bottomUp, reuseIdx="
                  + reuseIdx);
        }
      } else {
        if (debug) {
          System.out.println(">>>>adding the sliding segment from right because len(w) < lb");
        }
        needBottomUp = true; // 加了新的数据进来意味着下一轮里就要re-bottomUp

        while ((buffer_endIdx - buffer_startIdx) < lb && (points.size() - buffer_endIdx) > 0) {
          // 从0开始，并且rx是从0开始的右闭 [0,rx]一共rx+1个点
          List<Point> remaining_points = points.subList(buffer_endIdx, points.size());
          int rx;
          if (prefixSum == null) {
            rx = nextSlidingWindowWithTimestamps(remaining_points, maxError, null);
          } else {
            // deprecated
            double[] prefixSumX =
                Arrays.copyOfRange((double[]) prefixSum[0], buffer_endIdx, points.size());
            double[] prefixSumY =
                Arrays.copyOfRange((double[]) prefixSum[1], buffer_endIdx, points.size());
            double[] prefixSumX2 =
                Arrays.copyOfRange((double[]) prefixSum[2], buffer_endIdx, points.size());
            double[] prefixSumY2 =
                Arrays.copyOfRange((double[]) prefixSum[3], buffer_endIdx, points.size());
            double[] prefixSumXY =
                Arrays.copyOfRange((double[]) prefixSum[4], buffer_endIdx, points.size());
            if (buffer_endIdx > 0) {
              for (int i = 0; i < prefixSumX.length; i++) {
                prefixSumX[i] -= ((double[]) prefixSum[0])[buffer_endIdx - 1];
              }
              for (int i = 0; i < prefixSumY.length; i++) {
                prefixSumY[i] -= ((double[]) prefixSum[1])[buffer_endIdx - 1];
              }
              for (int i = 0; i < prefixSumX2.length; i++) {
                prefixSumX2[i] -= ((double[]) prefixSum[2])[buffer_endIdx - 1];
              }
              for (int i = 0; i < prefixSumY2.length; i++) {
                prefixSumY2[i] -= ((double[]) prefixSum[3])[buffer_endIdx - 1];
              }
              for (int i = 0; i < prefixSumXY.length; i++) {
                prefixSumXY[i] -= ((double[]) prefixSum[4])[buffer_endIdx - 1];
              }
            }
            Object[] prefixSumPart =
                new Object[] {
                  prefixSumX,
                  prefixSumY,
                  prefixSumX2,
                  prefixSumY2,
                  prefixSumXY,
                  prefixSum[5],
                  prefixSum[6]
                };
            rx = nextSlidingWindowWithTimestamps(remaining_points, maxError, prefixSumPart);
            // TODO prefixsum加速 但是由于精度问题，再加上maxError控制，结果有时候不会完全相同
          }
          if ((buffer_endIdx - buffer_startIdx) + (rx + 1) > ub) {
            // # avoid more than ub
            rx = ub - (buffer_endIdx - buffer_startIdx) - 1;
          }
          buffer_endIdx += (rx + 1); // buffer终点右移
          if (debug) {
            System.out.println("input len=" + (rx + 1));
          }
        }

        if (debug) {
          System.out.println(
              "buffer data: ["
                  + buffer_startIdx
                  + ","
                  + buffer_endIdx
                  + ")"
                  + ", remaining data: ["
                  + buffer_endIdx
                  + ","
                  + points.size()
                  + ")");
          if ((buffer_endIdx - buffer_startIdx) < lb) {
            System.out.println("warn less");
          }
          if ((buffer_endIdx - buffer_startIdx) > ub) {
            System.out.println("warn more");
          }
        }
      }
    }
    return segTs;
  }

  public static void main(String[] args) throws IOException {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = false;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 100_0000;
    //        Polyline polyline = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    Polyline polyline = new Polyline();
    for (int i = 0; i < N; i += 1) {
      double v = rand.nextInt(1000);
      polyline.addVertex(new Point(i, v));
    }
    try (FileWriter writer = new FileWriter("raw.csv")) {
      // 写入CSV头部
      writer.append("x,y,z\n");

      // 写入每个点的数据
      for (int i = 0; i < polyline.size(); i++) {
        Point point = polyline.get(i);
        writer.append(point.x + "," + point.y + "," + point.z + "\n");
      }
      System.out.println(polyline.size() + " Data has been written");
    } catch (IOException e) {
      System.out.println("Error writing to CSV file: " + e.getMessage());
    }

    //        Object[] prefixSum = prefixSum(polyline.getVertices());

    double maxError = 20000000;
    int m = 1000;

    // 33s worst-case O(n2)因为没有prefix sum加速计算L2误差，但是实际达不到worst-case所以实际运行不会那么大耗时
    long startTime = System.currentTimeMillis();
    //        List<Point> sampled = swab_framework(polyline.getVertices(), maxError, m, null,
    // false);
    List<Point> sampled = reducePoints(polyline.getVertices(), maxError, m);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    //        for (Point p : sampled) {
    //            System.out.println(p);
    //        }
    System.out.println(sampled.size());

    try (PrintWriter writer = new PrintWriter(new File("output.csv"))) {
      // 写入字符串
      for (int i = 0; i < sampled.size(); i++) {
        writer.println(sampled.get(i).x + "," + sampled.get(i).y);
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
