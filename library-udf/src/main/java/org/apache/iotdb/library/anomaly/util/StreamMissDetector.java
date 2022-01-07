/*
 * Copyright © 2021 iotdb-quality developer group (iotdb-quality@protonmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.quality.anomaly.util;

import org.apache.iotdb.quality.util.BooleanCircularQueue;
import org.apache.iotdb.quality.util.DoubleCircularQueue;
import org.apache.iotdb.quality.util.LongCircularQueue;

import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * 针对缺失异常的流式异常检测
 *
 * @author Wang Haoyu
 */
public class StreamMissDetector {

  private final LongCircularQueue timeWindow = new LongCircularQueue(); // 存放当前窗口内的数据点的时间戳
  private final DoubleCircularQueue valueWindow = new DoubleCircularQueue(); // 存放当前窗口内的数据点的值
  private final LongCircularQueue timeBuffer = new LongCircularQueue(); // 缓存输出数据点的时间戳
  private final BooleanCircularQueue labelBuffer = new BooleanCircularQueue(); // 缓存输出数据点的值
  private final SimpleRegression regression = new SimpleRegression(); // 线性回归器
  private final double threshold = 0.9999; // 完美线性阈值
  private int minLength; // 缺失异常长度的最小值
  private int state; // 状态码
  private long startTime; // 开始时间
  private int missingStartIndex;
  private boolean horizon;
  private double standard;

  public StreamMissDetector(int minLength) {
    this.state = 0;
    this.startTime = -1;
    this.minLength = minLength;
  }

  public void insert(long time, double value) {
    timeWindow.push(time);
    valueWindow.push(value);
    if (startTime < 0) {
      startTime = time;
    }
    switch (state) {
      case 0:
        if (timeWindow.getSize() >= getWindowSize()) {
          linearRegress(0, getWindowSize());
          double alpha = regression.getRSquare();
          if (Double.isNaN(alpha) || alpha > threshold) { // 发现线性片段
            missingStartIndex = 0;
            state = 2;
          } else { // 未发现线性片段
            regression.clear();
            state = 1;
          }
        }
        break;
      case 1:
        if (timeWindow.getSize() >= getWindowSize() * 2) {
          linearRegress(getWindowSize(), getWindowSize() * 2);
          double alpha = regression.getRSquare();
          if (Double.isNaN(alpha) || alpha > threshold) { // 发现线性片段
            missingStartIndex = backExtend(); // 前向扩展
            state = 2;
          } else { // 未发现线性片段
            for (int i = 0; i < getWindowSize(); i++) { // 前一个窗口中的所有数据点都不是异常，输出标记
              timeBuffer.push(timeWindow.pop());
              labelBuffer.push(false);
              valueWindow.pop();
            }
            regression.clear();
            state = 1;
          }
        }
        break;
      case 2:
        regression.addData(time - startTime, value);
        double alpha = regression.getRSquare();
        if ((horizon && value != standard) || (!horizon && alpha < threshold)) { // 后向扩展终止
          int missingEndIndex = timeWindow.getSize() - 1;
          for (int i = 0; i < missingStartIndex; i++) {
            timeBuffer.push(timeWindow.pop());
            labelBuffer.push(false);
            valueWindow.pop();
          }
          boolean label = missingEndIndex - missingStartIndex > minLength;
          for (int i = missingStartIndex; i < missingEndIndex; i++) {
            timeBuffer.push(timeWindow.pop());
            labelBuffer.push(label);
            valueWindow.pop();
          }
          regression.clear();
          state = 0;
        }
    }
  }

  public void flush() {
    switch (state) {
      case 0:
      case 1:
        while (!timeWindow.isEmpty()) {
          timeBuffer.push(timeWindow.pop());
          labelBuffer.push(false);
          valueWindow.pop();
        }
        break;
      case 2:
        boolean label = timeWindow.getSize() - missingStartIndex > minLength;
        for (int i = 0; i < missingStartIndex; i++) {
          timeBuffer.push(timeWindow.pop());
          labelBuffer.push(false);
          valueWindow.pop();
        }
        while (!timeWindow.isEmpty()) {
          timeBuffer.push(timeWindow.pop());
          labelBuffer.push(label);
          valueWindow.pop();
        }
    }
  }

  private int backExtend() {
    horizon = Double.isNaN(regression.getRSquare()); // 拟合得到的曲线是否是水平的
    standard = regression.getIntercept();
    int bindex = getWindowSize();
    while (bindex > 0) {
      bindex--;
      regression.addData(timeWindow.get(bindex) - startTime, valueWindow.get(bindex));
      double alpha = regression.getRSquare();
      if ((horizon && valueWindow.get(bindex) != standard) || (!horizon && alpha < threshold)) {
        break;
      }
    }
    regression.removeData(timeWindow.get(bindex) - startTime, valueWindow.get(bindex));
    return bindex + 1;
  }

  private void linearRegress(int start, int end) {
    double data[][] = new double[getWindowSize()][2];
    for (int i = start; i < end; i++) {
      data[i - start][0] = timeWindow.get(i) - startTime;
      data[i - start][1] = valueWindow.get(i);
    }
    regression.addData(data);
  }

  /**
   * 输出缓冲中是否存在下一个数据点
   *
   * @return 存在下一个数据点返回true，否则返回false
   */
  public boolean hasNext() {
    return !timeBuffer.isEmpty();
  }

  /**
   * 返回输出缓冲中当前数据点的时间戳
   *
   * @return 时间戳
   */
  public long getOutTime() {
    return timeBuffer.getHead();
  }

  /**
   * 返回输出缓冲中当前数据点的值
   *
   * @return 值
   */
  public boolean getOutValue() {
    return labelBuffer.getHead();
  }

  /** 在输出缓冲中移动到下一个数据点 */
  public void next() {
    timeBuffer.pop();
    labelBuffer.pop();
  }

  private int getWindowSize() {
    return minLength / 2;
  }

  /** @return the minLength */
  public int getMinLength() {
    return minLength;
  }

  /** @param minLength the minLength to set */
  public void setMinLength(int minLength) {
    this.minLength = minLength;
  }
}
