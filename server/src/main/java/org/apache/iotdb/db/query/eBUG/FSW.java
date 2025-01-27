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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.Tool.getParam;

// Liu, Xiaoyan, Lin, Zhenjiang, Wang, Huaiqing, 2008. Novel online methods for time series
// segmentation.
// FSW: feasible space window
// adapted from https://github.com/zhourongleiden/AEPLA.git
public class FSW {

  public static List<Point> reducePoints(List<Point> points, double epsilon, Object... kwargs) {
    int length = points.size();

    // Precompute upper and lower bounds
    double[] p_upper = new double[length];
    double[] p_lower = new double[length];
    for (int j = 0; j < length; j++) {
      double v = points.get(j).getValue();
      p_upper[j] = v + epsilon;
      p_lower[j] = v - epsilon;
    }

    // init the first segment
    int i = 0;
    int seg_no = 0;
    int csp_id = 0;
    List<int[]> segmentPoint = new ArrayList<>();
    segmentPoint.add(new int[] {0, 0});
    double upSlope = Double.POSITIVE_INFINITY;
    double lowSlope = Double.NEGATIVE_INFINITY;

    while (i < length - 1) {
      //      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      i++;
      upSlope =
          Math.min(
              upSlope,
              ((p_upper[i] - points.get(segmentPoint.get(seg_no)[1]).getValue())
                  / (points.get(i).getTimestamp()
                      - points.get(segmentPoint.get(seg_no)[1]).getTimestamp())));
      lowSlope =
          Math.max(
              lowSlope,
              ((p_lower[i] - points.get(segmentPoint.get(seg_no)[1]).getValue())
                  / (points.get(i).getTimestamp()
                      - points.get(segmentPoint.get(seg_no)[1]).getTimestamp())));
      if (upSlope < lowSlope) {
        seg_no += 1;
        segmentPoint.add(new int[] {seg_no, csp_id});
        i = csp_id;
        upSlope = Double.POSITIVE_INFINITY;
        lowSlope = Double.NEGATIVE_INFINITY;
      } else {
        double s =
            (points.get(i).getValue() - points.get(segmentPoint.get(seg_no)[1]).getValue())
                / (points.get(i).getTimestamp()
                    - points.get(segmentPoint.get(seg_no)[1]).getTimestamp());
        if (s >= lowSlope && s <= upSlope) {
          csp_id = i;
        }
      }
    }

    // deal with the last segment
    if (segmentPoint.get(seg_no)[1] < length - 1) {
      //      IOMonitor2.DCP_D_getAllSatisfiedPageData_traversedPointNum++;
      seg_no += 1;
      segmentPoint.add(new int[] {seg_no, length - 1});
    }

    List<Point> result = new ArrayList<>();
    for (int[] ints : segmentPoint) {
      result.add(points.get(ints[1]));
    }

    return result;
  }

  public static void main(String[] args) throws IOException {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = false;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 100000_0;
    List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    //        Polyline polyline = new Polyline();
    //        for (int i = 0; i < N; i += 1) {
    //            double v = rand.nextInt(1000);
    //            polyline.addVertex(new Point(i, v));
    //        }
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

    int m = 119;

    long startTime = System.currentTimeMillis();
    double epsilon = getParam(points, m, FSW::reducePoints, m * 0.01);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    List<Point> sampled = reducePoints(points, epsilon);
    System.out.println(sampled.size());
    System.out.println(epsilon);

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
