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
package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.library.util.Util;

import java.util.PriorityQueue;

/**
 * 利用LsGreedy方法进行数值修复的类
 *
 * @author Wang Haoyu
 */
public class LsGreedy extends ValueRepair {

  private double center = 0, sigma;
  private final double eps = 1e-12;

  public LsGreedy(RowIterator dataIterator) throws Exception {
    super(dataIterator);
    setParameters();
  }

  public LsGreedy(String filename) throws Exception {
    super(filename);
    setParameters();
  }

  private void setParameters() {
    double[] speed = Util.speed(original, time);
    double[] speedchange = Util.variation(speed);
    sigma = Util.mad(speedchange);
  }

  @Override
  public void repair() {
    repaired = original.clone();
    RepairNode table[] = new RepairNode[n];
    PriorityQueue<RepairNode> heap = new PriorityQueue<>();
    for (int i = 1; i < n - 1; i++) {
      RepairNode node = new RepairNode(i);
      table[i] = node;
      if (Math.abs(node.getU() - center) > 3 * sigma) {
        heap.add(node);
      }
    } // 初始化，填充堆
    while (true) {
      RepairNode top = heap.peek();
      if (top == null || Math.abs(top.getU() - center) < Math.max(eps, 3 * sigma)) {
        break;
      } // 堆是空的，或者所有的速度变化都在center±3sigma范围内，停止贪心
      top.modify();
      for (int i = Math.max(1, top.getIndex() - 1); i <= Math.min(n - 2, top.getIndex() + 1); i++) {
        heap.remove(table[i]);
        RepairNode temp = new RepairNode(i);
        table[i] = temp;
        if (Math.abs(temp.getU() - center) > 3 * sigma) {
          heap.add(temp);
        }
      }
    } // 贪婪地寻找u与center差距最大的点并进行修复
  }

  class RepairNode implements Comparable<RepairNode> {

    private final int index; // 被修复的数据点的编号
    private final double u; // 当前的速度变化

    public RepairNode(int index) {
      this.index = index;
      double v1 = repaired[index + 1] - repaired[index];
      v1 = v1 / (time[index + 1] - time[index]);
      double v2 = repaired[index] - repaired[index - 1];
      v2 = v2 / (time[index] - time[index - 1]);
      this.u = v1 - v2;
    }

    /** 调整被修复的数据点的值，使得其速度变化与center的差距下降一个sigma */
    public void modify() {
      double temp;
      if (sigma < eps) {
        temp = Math.abs(u - center);
      } else {
        temp = Math.max(sigma, Math.abs(u - center) / 3);
      }
      temp *=
          (time[index + 1] - time[index])
              * (time[index] - time[index - 1])
              / (time[index + 1] - time[index - 1]);
      if (this.u > center) {
        repaired[index] += temp;
      } else {
        repaired[index] -= temp;
      }
    }

    @Override
    public int compareTo(RepairNode o) {
      double u1 = Math.abs(this.u - center);
      double u2 = Math.abs(o.u - center);
      if (u1 > u2) {
        return -1;
      } else if (u1 == u2) {
        return 0;
      } else {
        return 1;
      }
    }

    /** @return the index */
    public int getIndex() {
      return index;
    }

    /** @return the u */
    public double getU() {
      return u;
    }
  }

  /** @param center the center to set */
  public void setCenter(double center) {
    this.center = center;
  }

  /** @param sigma the sigma to set */
  public void setSigma(double sigma) {
    this.sigma = sigma;
  }

  public static void main(String[] args) throws Exception {
    LsGreedy lsGreedy = new LsGreedy("temp.csv");
    lsGreedy.repair();
    for (int i = 0; i < lsGreedy.n; i++) {
      System.out.println(lsGreedy.time[i] + " " + lsGreedy.repaired[i]);
    }
  }
}
