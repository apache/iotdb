package org.apache.iotdb.db.query.eBUG;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

// Some public references about dynamic programming:
// https://justinwillmert.com/articles/2014/bellman-k-segmentation-algorithm/
// https://github.com/CrivelliLab/Protein-Structure-DL

public class DP { // Dynamic-Programming

  // Enum to represent error types
  public enum ERRORtype {
    L1,
    L2,
    L_infy,
    area
  }

  // precomputing error measures of all possible segments, e.g., ad2 table
  public static double[][] prepareCostTable(
      List<Point> points, ERRORtype errorType, boolean debug) {
    int N = points.size();
    double[][] dists = new double[N][N]; // 默认0，对角线元素已经都是0了
    // 一个 double[47700][47700] 矩阵大约需要 16.94 GB 的内存。

    // 外层循环i：range长度=right boundary-left boundary
    // 内层循环j: 矩阵逐行
    for (int i = 1; i < N; i++) { // TODO
      for (int j = 0; j < N - i; j++) {
        // j = left boundary, r = right boundary, 左闭右闭
        int r = i + j; // r<N
        if (debug) {
          System.out.println(">>>> i=" + i + ", j=" + j + ", r=" + r);
        }

        // lx=j,rx=r, 从lx=j到rx=r（左闭右闭）的linear interpolation（即连接lx和rx点）的errorType类型的误差
        double mc = joint_segment_error(points, j, r, errorType);
        dists[j][r] = mc;
        if (debug) {
          System.out.println("dists=");
          Tool.printArray(dists);
          System.out.println("---------------------");
        }
      }
    }
    return dists;
  }

  // 注意k是分段数，不是采点数
  public static List<Point> dynamic_programming(
      List<Point> points, int k, ERRORtype errorType, boolean debug) {
    int N = points.size();

    // 预计算dists（即ad2 table），dists[i][j]代表对子序列i:j左闭右闭直接连接pi和pj的近似误差
    double[][] dists = prepareCostTable(points, errorType, debug);
    if (debug) {
      Tool.printArray(dists);
      System.out.println("----------------------------------");
    }

    // 创建dp table。kSegDist[i][j]代表把子序列T[0:j]左闭右闭分成i段(java从0开始计数)的最小误差
    double[][] kSegDist = new double[k][N];
    // Initialize the case k=1 directly from the pre-computed distances
    // 注意java从0开始，所以是0 index第一行代表分段数1
    System.arraycopy(dists[0], 0, kSegDist[0], 0, kSegDist[0].length);
    if (debug) {
      System.out.println("k_seg_dist=");
      Tool.printArray(kSegDist);
    }

    // 创建path table。记录子问题的解。kSegPath[i][j]代表把T[0:j]分成i段这个问题的最优子问题的位置
    // 子问题是把T[0:kSegPath[i][j]]分成i-1段，以及最后一段T[kSegPath[i][j]:end]
    int[][] kSegPath = new int[k][N];
    // 第一行只分一段，所以不管终点是哪个（列），左边的闭合起点都是0
    // 虽然java数组默认初始是0，但是这里还是显式赋值
    Arrays.fill(kSegPath[0], 0);
    if (debug) {
      System.out.println("k_seg_path=");
      Tool.printArray(kSegPath);
    }

    // 外层循环i：分段数，注意python从0开始，所以实际含义是2~k个分段数
    for (int i = 1; i < k; i++) {
      // 内层循环j：从第一个点开始到j终点（闭合）的序列
      // 所以含义是：找到从第一个点开始到j终点（闭合）的序列的分成(i+1)段的最佳分段方案（误差最小）
      for (int j = 0; j < N; j++) {
        // 动态规划
        // 注意linear interpolation不需要单点成为一个分段的情况
        // TODO 误差类型
        // TODO ghost修复
        // 从0:j的序列分成i段的问题：遍历所有可能的子问题组合解
        if (debug) {
          System.out.println(">>>分段数i+1" + (i + 1) + ",end pos j=" + j);
        }
        double[] choices = new double[j + 1]; // java从0开始
        int bestIndex = -1;
        double bestVal = Double.MAX_VALUE; // 越小越好
        for (int xtmp = 0; xtmp < j + 1; xtmp++) {
          if (errorType == ERRORtype.L_infy) {
            choices[xtmp] = Math.max(kSegDist[i - 1][xtmp], dists[xtmp][j]);
          } else {
            choices[xtmp] = kSegDist[i - 1][xtmp] + dists[xtmp][j];
          }
          if (choices[xtmp] < bestVal) {
            bestVal = choices[xtmp];
            bestIndex = xtmp;
          }
        }
        if (debug) {
          for (int xtmp = 0; xtmp < j + 1; xtmp++) { // 遍历从 0 到 j 的每个元素
            if (errorType == ERRORtype.L_infy) {
              System.out.printf(
                  "  max((k_seg_dist[%d, %d] = %f), (dists[%d, %d] = %f)) --> %f%n",
                  i - 1,
                  xtmp,
                  kSegDist[i - 1][xtmp],
                  xtmp,
                  j,
                  dists[xtmp][j],
                  Math.max(kSegDist[i - 1][xtmp], dists[xtmp][j]));
            } else {
              System.out.printf(
                  "  (k_seg_dist[%d, %d] = %f) + (dists[%d, %d] = %f) --> %f%n",
                  i - 1,
                  xtmp,
                  kSegDist[i - 1][xtmp],
                  xtmp,
                  j,
                  dists[xtmp][j],
                  kSegDist[i - 1][xtmp] + dists[xtmp][j]);
            }
          }
        }

        // Store the sub-problem solution
        kSegPath[i][j] = bestIndex;
        kSegDist[i][j] = bestVal;

        if (debug) {
          System.out.println("kSegDist[" + i + "][" + j + "] = " + bestVal);
          System.out.println("kSegDist=");
          Tool.printArray(kSegDist);
          System.out.println("kSegPath=");
          Tool.printArray(kSegPath);
        }
      }
    }

    // 开始回溯构建采样结果
    List<Point> sDs = new ArrayList<>(); // k+1个采样点
    List<Integer> sDs_idx = new ArrayList<>();
    int rhs = N - 1;
    sDs.add(points.get(rhs)); // 全局尾点
    sDs_idx.add(rhs);

    for (int i = k - 1; i >= 0; i--) {
      int lhs = kSegPath[i][rhs];
      sDs.add(points.get(lhs));
      sDs_idx.add(lhs);
      rhs = lhs;
    }

    // 反转列表
    Collections.reverse(sDs);
    Collections.reverse(sDs_idx);

    if (debug) {
      System.out.println(sDs);
      System.out.println(">>>>>ad2[][]=");
      Tool.printArray(dists);
      System.out.println(">>>>>dp[][]=");
      Tool.printTransposeArray(kSegDist);
      System.out.println(">>>>>path[][]=");
      Tool.printTransposeArray(kSegPath);
      System.out.println(error(points, sDs_idx, errorType));
    }

    return sDs;
  }

  //    // Helper method to get index of minimum value
  //    private static int getIndexOfMin(double[] array) {
  //        double minVal = array[0];
  //        int minIndex = 0;
  //        for (int i = 1; i < array.length; i++) {
  //            if (array[i] < minVal) {
  //                minVal = array[i];
  //                minIndex = i;
  //            }
  //        }
  //        return minIndex;
  //    }

  // Method to calculate joint segment error based on error type
  public static double joint_segment_error(
      List<Point> points, int lx, int rx, ERRORtype errorType) {
    // 默认joint=true, residual=true，即使用linear interpolation近似分段
    // lx~rx 左闭右闭
    if (lx == rx) {
      return 0;
    }

    if (errorType != ERRORtype.area) {
      double x1 = points.get(lx).x;
      double y1 = points.get(lx).y;
      double x2 = points.get(rx).x;
      double y2 = points.get(rx).y;

      // linear interpolation:
      double k = (y2 - y1) / (x2 - x1);
      double b = (y1 * x2 - y2 * x1) / (x2 - x1);

      double tmp = 0;
      if (errorType == ERRORtype.L2) {
        for (int i = lx; i <= rx; i++) {
          double e =
              (k * points.get(i).x + b - points.get(i).y)
                  * (k * points.get(i).x + b - points.get(i).y);
          tmp += e;
        }
      } else if (errorType == ERRORtype.L1) {
        for (int i = lx; i <= rx; i++) {
          double e = Math.abs(k * points.get(i).x + b - points.get(i).y);
          tmp += e;
        }
      } else if (errorType == ERRORtype.L_infy) {
        for (int i = lx; i <= rx; i++) {
          double e = Math.abs(k * points.get(i).x + b - points.get(i).y); // 注意绝对值
          if (e > tmp) {
            tmp = e;
          }
        }
      }
      return tmp;
    } else { // AD
      List<Point> linearInterpolation = new ArrayList<>();
      linearInterpolation.add(points.get(lx));
      linearInterpolation.add(points.get(rx));
      return Tool.total_areal_displacement(
          points.subList(lx, rx + 1), // 注意lx,rx左闭右闭
          linearInterpolation,
          false);
    }
  }

  public static double error(List<Point> points, List<Integer> sampledIdx, ERRORtype errorType) {
    double res = 0;
    for (int i = 0; i < sampledIdx.size() - 1; i++) {
      int lx = sampledIdx.get(i);
      int rx = sampledIdx.get(i + 1);
      double e = joint_segment_error(points, lx, rx, errorType);
      if (errorType == ERRORtype.L_infy) {
        res = Math.max(res, e);
      } else {
        res += e;
      }
    }
    return res;
  }

  // Example usage
  public static void main(String[] args) {
    Random rand = new Random(10);
    String input =
        "D:\\LabSync\\iotdb\\我的Gitbook基地\\RUI Lei gitbook\\士论\\49. visval改进\\notebook\\raw.csv";
    boolean hasHeader = true;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 100;
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
      for (Point point : points) {
        writer.append(point.x + "," + point.y + "," + point.z + "\n");
      }
      System.out.println(points.size() + " Data has been written");
    } catch (IOException e) {
      System.out.println("Error writing to CSV file: " + e.getMessage());
    }

    long startTime = System.currentTimeMillis();
    //        double[][] ad2 = prepareKSegments(points, ERROR.L1, false);
    int m = 10;
    List<Point> sampled = dynamic_programming(points, m - 1, ERRORtype.area, false);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    for (Point p : sampled) {
      System.out.println(p);
    }
    System.out.println(sampled.size());

    //        try (PrintWriter writer = new PrintWriter(new File("output.csv"))) {
    //            // 写入字符串
    //            for (int i = 0; i < sampled.size(); i++) {
    //                writer.println(sampled.get(i).x + "," + sampled.get(i).y);
    //            }
    //
    //        } catch (FileNotFoundException e) {
    //            e.printStackTrace();
    //        }
  }
}
