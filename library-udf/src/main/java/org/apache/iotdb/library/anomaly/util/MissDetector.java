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
package org.apache.iotdb.library.anomaly.util;

import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.access.RowIterator;
import org.apache.iotdb.library.util.Util;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.util.ArrayList;

/**
 * 针对缺失异常的异常检测
 *
 * @author Wang Haoyu
 */
public class MissDetector {

  private LongArrayList time = new LongArrayList();
  private DoubleArrayList value = new DoubleArrayList();
  private IntArrayList predictLabel = new IntArrayList();
  private int len;

  private int minLength;
  private double threshold = 0.9999;
  private double lineBoundary = 0.6;
  private long startTime;
  private int windowCnt = 0; // 统计window数目的计数器
  private double r2 = 0; // 统计全段数据的平均R^2
  private ArrayList<MissingSubSeries> anomalyList = new ArrayList<>(); // 存储所有异常片段

  public MissDetector(RowIterator iterator, int minLength) throws Exception {
    while (iterator.hasNextRow()) {
      Row row = iterator.next();
      double v = Util.getValueAsDouble(row);
      if (Double.isFinite(v)) {
        this.time.add(row.getTime());
        this.value.add(v);
      }
    }
    this.len = this.time.size();
    this.minLength = minLength;
  }

  public void detect() {
    getPredictLabel().addAll(new int[getLen()]); // 将全部预测标签都初始化为0
    this.startTime = getTime().get(0); // 初始化全局开始时间
    int i = 0, windowSize = minLength / 2;
    double data[][] = new double[windowSize][2];
    SimpleRegression regression = new SimpleRegression();
    while (i + windowSize < getLen()) {
      // 准备窗口内的数据
      for (int j = 0; j < windowSize; j++) {
        data[j][0] = getTime().get(i + j) - startTime;
        data[j][1] = getValue().get(i + j);
      }
      // 线性拟合
      regression.addData(data);
      double alpha = regression.getRSquare();
      if (Double.isNaN(alpha) || alpha >= threshold) { // R2为NAN是因为斜率为0
        i = extend(regression, i, i + windowSize);
      } else {
        i += windowSize;
      }
      regression.clear();
      r2 += Double.isNaN(alpha) ? 1 : alpha;
      windowCnt++;
    }
    label(); // 标记异常
  }

  /**
   * 基于已发现的线性窗口向前后扩展，寻找局部最大的线性窗口
   *
   * @param regression 线性回归器
   * @param start 已发现的窗口的起始索引（包含）
   * @param end 已发现的窗口的末尾索引（不包含）
   * @return 扩展后的窗口的末尾索引（不包含）
   */
  private int extend(SimpleRegression regression, int start, int end) {
    boolean horizon = Double.isNaN(regression.getRSquare()); // 拟合得到的曲线是否是水平的
    double standard = regression.getIntercept();
    // 向前扩展
    int bindex = start;
    while (bindex > 0) {
      bindex--;
      regression.addData(getTime().get(bindex) - startTime, getValue().get(bindex));
      double alpha = regression.getRSquare();
      if ((horizon && getValue().get(bindex) != standard) || (!horizon && alpha < threshold)) {
        break;
      }
    }
    regression.removeData(getTime().get(bindex) - startTime, getValue().get(bindex));
    if (bindex == 0) { // 缺失异常应该发生在数据中间，而不是数据的头部
      return end;
    }
    // 向后扩展
    int findex = end;
    while (findex < getLen()) {
      regression.addData(getTime().get(findex) - startTime, getValue().get(findex));
      double alpha = regression.getRSquare();
      if ((horizon && getValue().get(findex) != standard) || (!horizon && alpha < threshold)) {
        break;
      }
      findex++;
    }
    if (findex == getLen()) { // 缺失异常应该发生在数据中间，而不是数据的尾部
      return end;
    }
    // 标记异常子序列
    MissingSubSeries m = new MissingSubSeries(bindex + 1, findex);
    anomalyList.add(m);
    return findex;
  }

  /** 如果数据不是直线型的，则标记所有缺失异常的子序列 */
  private void label() {
    if (r2 / windowCnt < lineBoundary) {
      for (MissingSubSeries m : anomalyList) {
        if (m.getLength() >= minLength) {
          for (int i = m.getStart(); i < m.getEnd(); i++) {
            getPredictLabel().set(i, 1);
          }
        }
      }
    }
  }

  /** @return the minLength */
  public int getMinLength() {
    return minLength;
  }

  /** @param minLength the minLength to set */
  public void setMinLength(int minLength) {
    this.minLength = minLength;
  }

  /** @return the threshold */
  public double getThreshold() {
    return threshold;
  }

  /** @param threshold the threshold to set */
  public void setThreshold(double threshold) {
    this.threshold = threshold;
  }

  private class MissingSubSeries implements Comparable<MissingSubSeries> {

    private int start;
    private int end;
    private double slope;

    public MissingSubSeries() {}

    public MissingSubSeries(int start, int end) {
      this.start = start;
      this.end = end;
      this.slope =
          (getValue().get(end - 1) - getValue().get(start))
              / (getTime().get(end - 1) - getTime().get(start));
    }

    @Override
    public String toString() {
      return String.format(
          "Start: %d, End: %d, Length: %d, Slope: %.12f", start, end, end - start, slope);
    }

    public int getLength() {
      return end - start;
    }

    /** @return the start */
    public int getStart() {
      return start;
    }

    /** @param start the start to set */
    public void setStart(int start) {
      this.start = start;
    }

    /** @return the end */
    public int getEnd() {
      return end;
    }

    /** @param end the end to set */
    public void setEnd(int end) {
      this.end = end;
    }

    /** @return the slope */
    public double getSlope() {
      return slope;
    }

    /** @param slope the slope to set */
    public void setSlope(double slope) {
      this.slope = slope;
    }

    @Override
    public int compareTo(MissingSubSeries o) {
      if (this.getLength() > o.getLength()) {
        return -1;
      } else if (this.getLength() == o.getLength()) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  /** @return the predictLabel */
  public IntArrayList getPredictLabel() {
    return predictLabel;
  }

  /** @return the len */
  public int getLen() {
    return len;
  }

  /** @return the time */
  public LongArrayList getTime() {
    return time;
  }

  /** @return the value */
  public DoubleArrayList getValue() {
    return value;
  }
}
