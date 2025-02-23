package org.apache.iotdb.db.query.eBUG;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class BottomUpYdiff {

  public static List<Point> reducePoints(
      List<Point> points, int m, DP.ERRORtype errorType, boolean debug) throws IOException {
    if (m <= 2) {
      throw new IOException("please make m>2");
    }
    // 直接控制采样点数
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

      double mc = DP.joint_segment_error(points, index1, index3, errorType);

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

    int remainNonTerminalPoint = m - 2;
    int fakeCnt = 0;
    //        while (!triangleHeap.isEmpty() && triangleHeap.peek().area < maxError) { // TODO
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
            DP.joint_segment_error(points, tri.prev.indices[0], tri.prev.indices[2], errorType);

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
            DP.joint_segment_error(points, tri.next.indices[0], tri.next.indices[2], errorType);

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

  public static void main(String[] args) throws IOException {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = false;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 1000_00;
    //        List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    Polyline polyline = new Polyline();
    for (int i = 0; i < N; i += 1) {
      double v = rand.nextInt(1000);
      polyline.addVertex(new Point(i, v));
    }
    List<Point> points = polyline.getVertices();
    try (FileWriter writer = new FileWriter("raw.csv")) {
      // 写入CSV头部
      writer.append("x,y,z\n");

      // 写入每个点的数据
      for (int i = 0; i < points.size(); i++) {
        Point point = points.get(i);
        writer.append(point.x + "," + point.y + "," + point.z + "\n");
      }
      System.out.println(points.size() + " Data has been written");
    } catch (IOException e) {
      System.out.println("Error writing to CSV file: " + e.getMessage());
    }

    int m = 100;

    long startTime = System.currentTimeMillis();
    List<Point> sampled = reducePoints(points, m, DP.ERRORtype.L_infy, false);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");
    //        for (Point p : sampled) {
    //            System.out.println(p);
    //        }
    System.out.println(sampled.size());

    //        startTime = System.currentTimeMillis();
    ////        List<Point> sampled2 = eBUG.buildEffectiveArea(points, 100000000, false, m);
    //        List<Point> sampled2 = SWAB.seg_bottomUp_m_withTimestamps(points, m, null, false);
    //        endTime = System.currentTimeMillis();
    //        System.out.println("Time taken: " + (endTime - startTime) + "ms");
    ////        for (Point p : sampled2) {
    ////            System.out.println(p);
    ////        }
    //        System.out.println(sampled2.size());
    //        for (int i = 0; i < sampled2.size(); i++) {
    //            if (sampled.get(i).x != sampled2.get(i).x) {
    //                throw new IOException("wrong!");
    //            }
    //        }

    //    try (PrintWriter writer = new PrintWriter(new File("output.csv"))) {
    //      // 写入字符串
    //      for (int i = 0; i < sampled.size(); i++) {
    //        writer.println(sampled.get(i).x + "," + sampled.get(i).y);
    //      }
    //
    //    } catch (FileNotFoundException e) {
    //      e.printStackTrace();
    //    }
  }
}
