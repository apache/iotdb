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

package org.apache.iotdb.library.dprofile.util;

import org.apache.iotdb.library.util.CircularQueue;
import org.apache.iotdb.library.util.DoubleCircularQueue;
import org.apache.iotdb.library.util.LongCircularQueue;

import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
 * 用于时间序列重采样（上采样、下采样）的类 util for resample
 *
 * @author Wang Haoyu
 */
public class Resampler {

  private final LongArrayList timeWindow = new LongArrayList(); // timestamps in the window
  private final DoubleArrayList valueWindow = new DoubleArrayList(); // values in the window
  private final LongCircularQueue waitList = new LongCircularQueue(); // timestamp for interpolation
  private final CircularQueue<SourceDataPoint> source =
      new CircularQueue<>(); // value for interpolation
  private final LongCircularQueue timeBuffer =
      new LongCircularQueue(); // buffer for output timestamp gap
  private final DoubleCircularQueue valueBuffer =
      new DoubleCircularQueue(); // buffer for ouput value
  private final long newPeriod; // resampling period
  private final String aggregator; // method to aggregate
  private final String interpolator; // method to interpolate
  private long currentTime; // start time of the window, left close & right open
  private long startTime,
      endTime; // start time (contained) and end time (not contained) of resampling
  private boolean outer = true; // if to use outer interpolate

  public Resampler(long newPeriod, String aggregator, String interpolator) {
    this(newPeriod, aggregator, interpolator, -1, -1);
  }

  public Resampler(
      long newPeriod, String aggregator, String interpolator, long startTime, long endTime) {
    this.newPeriod = newPeriod;
    this.aggregator = aggregator;
    this.interpolator = interpolator;
    this.startTime = startTime;
    this.endTime = endTime;
    this.currentTime = this.startTime;
  }

  /** 加入新的数据点 insert new datapoint */
  public void insert(long time, double value) {
    if (Double.isNaN(value)
        || (startTime > 0 && time < startTime)
        || (endTime > 0 && time >= endTime)) { // 跳过值为NAN的数据和不在范围内的数据
      return;
    }
    if (currentTime < 0) { // 初始化窗口
      currentTime = time;
    }
    while (time >= currentTime + newPeriod) {
      downSample(); // 窗口已满，对其进行清理
      upSample();
      currentTime += newPeriod;
    }
    // 向窗口填充新的数据
    timeWindow.add(time);
    valueWindow.add(value);
  }

  /** 从文件中批量加入数据点 insert points from a file */
  public void insert(String filename) throws FileNotFoundException, ParseException {
    Scanner sc = new Scanner(new File(filename));
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sc.useDelimiter("\\s*(,|\\r|\\n)\\s*"); // delimited by ',' or '\n', may contains ' ' nearby.
    sc.nextLine();
    while (sc.hasNext()) {
      insert(format.parse(sc.next()).getTime(), sc.nextDouble());
    }
  }

  /** 强制对所有缓存数据进行处理 process all data in buffer */
  public void flush() {
    do { // process data in the last window in first cycle
      downSample();
      currentTime += newPeriod;
    } while (endTime >= currentTime);
    outer = true;
    upSample();
  }

  /** 上采样，利用插值算法处理值为空的点，并将处理完成的点输出 */
  private void upSample() {
    if (source.getSize() > 2 || (outer && source.getSize() == 2)) {
      if (source.getSize() > 2) {
        source.pop();
      }
      // interpolate data in waitlist
      while (!waitList.isEmpty()) {
        long t = waitList.getHead();
        if (!outer && source.get(1).time < t) { // no enough data for inner interpolate
          break;
        }
        if (endTime < 0 || t < endTime) { // no output for timestamp larger than end time
          timeBuffer.push(t);
          valueBuffer.push(interpolate(t)); // interpolate
        }
        waitList.pop();
      }
      outer = false;
    }
  }

  /**
   * 下采样，将窗口内的数据点进行聚合，并为上采样做准备 downsampling. aggregate data in the window, and prepare for
   * upsampling.
   */
  private void downSample() {
    if (timeWindow.size() >= 2) {
      // aggregate，add result and timestamp to source
      double result = aggregate();
      source.push(new SourceDataPoint(currentTime, result));
    } else if (timeWindow.size() == 1) {
      // add the mere data into source
      source.push(new SourceDataPoint(timeWindow.get(0), valueWindow.get(0)));
    }
    timeWindow.clear();
    valueWindow.clear();
    waitList.push(currentTime);
  }

  /**
   * 聚合。根据聚合算法，将窗口内的数据点聚合为一个值 aggregate data in the window to one value according to given method.
   */
  private double aggregate() {
    double ret;
    switch (aggregator) {
      case "min":
        ret = valueWindow.min();
        break;
      case "max":
        ret = valueWindow.max();
        break;
      case "mean":
        ret = valueWindow.average();
        break;
      case "median":
        ret = valueWindow.median();
        break;
      case "first":
        ret = valueWindow.get(0);
        break;
      case "last":
        ret = valueWindow.get(valueWindow.size() - 1);
        break;
      default:
        throw new IllegalArgumentException("Error: Illegal Aggregation Algorithm.");
    }
    return ret;
  }

  /** 插值。根据插值算法，计算给定时间戳对应的值 interpolate. transfer given timestamp to interpolated timestamp. */
  private double interpolate(long t) {
    if (t == source.get(1).time) {
      return source.get(1).value;
    } else if (t == source.get(0).time) {
      return source.get(0).value;
    }
    double ret = Double.NaN;
    switch (interpolator) {
      case "nan":
        ret = Double.NaN;
        break;
      case "ffill":
        if (t >= source.get(1).time) {
          ret = source.get(1).value;
        } else if (t >= source.get(0).time) {
          ret = source.get(0).value;
        }
        break;
      case "bfill":
        if (t <= source.get(0).time) {
          ret = source.get(0).value;
        } else if (t <= source.get(1).time) {
          ret = source.get(1).value;
        }
        break;
      case "linear":
        ret =
            source.get(0).value * (source.get(1).time - t)
                + source.get(1).value * (t - source.get(0).time);
        ret = ret / (source.get(1).time - source.get(0).time);
        break;
      default:
        throw new IllegalArgumentException("Error: Illegal Interpolation Algorithm.");
    }
    return ret;
  }

  /** 输出缓冲中是否存在下一个数据点 judge if there is a next point in the buffer */
  public boolean hasNext() {
    return !timeBuffer.isEmpty();
  }

  /** 返回输出缓冲中当前数据点的时间戳 return the timestamp of the current point in buffer */
  public long getOutTime() {
    return timeBuffer.getHead();
  }

  /** 返回输出缓冲中当前数据点的值 return the value of the current point in buffer */
  public double getOutValue() {
    return valueBuffer.getHead();
  }

  /** 在输出缓冲中移动到下一个数据点 move to next data point in buffer */
  public void next() {
    timeBuffer.pop();
    valueBuffer.pop();
  }

  private class SourceDataPoint {

    long time;
    double value;

    public SourceDataPoint(long time, double value) {
      this.time = time;
      this.value = value;
    }
  }

  public static void main(String[] args) throws FileNotFoundException, ParseException {
    Resampler resampler = new Resampler(1000 * 60 * 60 * 24, "median", "linear");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    resampler.currentTime = format.parse("2021-06-19 12:00:00").getTime();
    resampler.endTime = format.parse("2021-06-19 15:00:00").getTime();
    //        resampler.endTime = 20000;
    resampler.insert("resample.csv");
    //        for (int i=0; i< 10; i++){
    //            resampler.insert(i*1000, i);
    //        }
    resampler.flush();
    while (resampler.hasNext()) {
      System.out.print(format.format(new Date(resampler.getOutTime())));
      System.out.println(" , " + resampler.getOutValue());
      resampler.next();
    }
  }
}
