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

package org.apache.iotdb.tsfile.encoding;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.tsfile.encoding.encoder.SDTEncoder;
import org.junit.Test;

public class SDTEncoderTest {

  @Test
  public void testIntSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      int value = (int) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeInt(time, value)) {
        count++;
      }
    }
    assertEquals(22, count);
  }

  @Test
  public void testDoubleSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      double value = 10 * Math.sin(degree++ * 3.141592653589793D / 180.0D);
      if (encoder.encodeDouble(time, value)) {
        count++;
      }
    }
    assertEquals(14, count);
  }

  @Test
  public void mytest1() throws Exception {
    String csvData = "D:\\full-game\\BallSpeed.csv";
    int start = 200000;
    int range = 200;
    int end = start + range;
    SDTEncoder encoder = new SDTEncoder();
    double e = 80000.0 / 2; // std/2
    encoder.setCompDeviation(e / 2);
    long count = 0;
    String line;
    List<Long> timestampList = new ArrayList<>();
    List<Long> valueList = new ArrayList<>();
    List<Long> selectTimestamps = new ArrayList<>();
    List<Long> selectValues = new ArrayList<>();
    long idx = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(csvData))) {
      while ((line = reader.readLine()) != null) {
        count++;
        if (count >= start && count < end) {
          idx++;
//          long time = Long.parseLong(line.split(",")[0]);
          long time = idx;
          long value = Long.parseLong(line.split(",")[1]);
          timestampList.add(time);
          valueList.add(value);
          if (encoder.encodeLong(time, value)) {
            selectTimestamps.add(encoder.getTime());
            selectValues.add(encoder.getLongValue());
          }
          if (count == end - 1) { // last point
            selectTimestamps.add(time);
            selectValues.add(value);
          }
        } else if (count >= end) {
          break;
        }
      }
    }

    System.out.println("close all;clear all;clc;");
    System.out.println("e=" + e);
    System.out.println("t=" + timestampList + ";");
    System.out.println("v=" + valueList + ";");
    System.out.println("at=" + selectTimestamps + ";");
    System.out.println("av=" + selectValues + ";");

    System.out.println("LB=av-e;\n" + "UB=av+e;\n" + "plot(t,v)\n" + "hold on,plot(t,v,'+')\n"
        + "hold on,plot(at,av,'r')\n" + "%hold on,plot(at,av,'r+')\n" + "hold on,plot(at,UB,'g')\n"
        + "%hold on,plot(at,UB,'g+')\n" + "hold on,plot(at,LB,'g')\n"
        + "%hold on,plot(at,LB,'g+')\n" + "\n" + "% [val,idx]=max(av-e)\n" + "% yline(val)\n" + "\n"
        + "[sortedX, sortedInds] = sort(LB,'descend');\n" + "% yline(sortedX(1))\n"
        + "% yline(sortedX(2))\n" + "\n" + "% 依次计算UB小于Nth-max-LB的点数\n"
        + "% case 1: 两端都小于threshold，则该段内的点的UB都小于threshold，都可以被剪枝\n"
        + "% case 2: 两端都大于等于threshold，则该段内的点的UB都大于等于threshold，则都不可以被剪枝\n"
        + "% case 3: 一端小于一端大于等于threshold，则该段内有部分点的UB小于threshold可以被剪枝\n"
        + "% (t1,v1), (t2,v2), t1<t2\n" + "% y=(t-t1)*(v2-v1)/(t2-t1)+v1\n"
        + "% y<thresold ===> (t-t1)*(v2-v1)/(t2-t1)+v1<threshold\n"
        + "% 如果v2>=threshold>v1: 则t<(threshold-v1)*(t2-t1)/(v2-v1)+t1，于是[t1,(threshold-v1)*(t2-t1)/(v2-v1)+t1)内的点可剪枝\n"
        + "% 如果v2<threshold<=v1: 则t>(threshold-v1)*(t2-t1)/(v2-v1)+t1，于是((threshold-v1)*(t2-t1)/(v2-v1)+t1,t2]内的点可剪枝\n"
        + "rank=1\n" + "threshold=sortedX(rank)\n" + "hold on, yline(threshold);\n"
        + "hold on, plot(at(sortedInds(rank)),threshold,'o')\n" + "\n" + "prune_t=[]; % point\n"
        + "prune_v=[]; % point\n" + "prune_interval=[]; % interval\n" + "interval_start=-1;\n"
        + "for i=2:1:length(av)\n" + "\tt1=at(i-1);\n" + "\tt2=at(i);\n" + "\tv1=UB(i-1);\n"
        + "\tv2=UB(i);\n" + "\ta=[];\n" + "\tif v1<threshold && v2<threshold\n"
        + "\t\tif i<length(av)\n" + "\t\t\ta=t1:1:t2-1;\n" + "\t\telse \n" + "\t\t\ta=t1:1:t2;\n"
        + "\t\tend\n" + "\t\tif interval_start<0\n" + "\t\t\tinterval_start=t1;\n" + "        end\n"
        + "        interval_end=t2; % continuous\n" + "\telseif v1<threshold && v2>=threshold\n"
        + "\t\ta=t1:1:floor((threshold-v1)*(t2-t1)/(v2-v1)+t1); % no need -1 here\n"
        + "\t\tif interval_start<0\n" + "\t\t\tinterval_start=t1;\n" + "        end\n"
        + "        prune_interval=[prune_interval;[interval_start,floor((threshold-v1)*(t2-t1)/(v2-v1)+t1)]];\n"
        + "\t\tinterval_start=-1; % discontinuous\n" + "\telseif v1>=threshold && v2<threshold\n"
        + "\t\tif i<length(av)\n" + "\t\t\ta=ceil((threshold-v1)*(t2-t1)/(v2-v1)+t1):1:t2-1;\n"
        + "\t\telse\n" + "\t\t\ta=ceil((threshold-v1)*(t2-t1)/(v2-v1)+t1):1:t2;\n" + "\t\tend\n"
        + "\t\tinterval_start=ceil((threshold-v1)*(t2-t1)/(v2-v1)+t1);\n" + "\t\tinterval_end=t2;\n"
        + "\tend\n" + "\tprune_t=[prune_t,a];\n"
        + "\tprune_v=[prune_v,(a-t1)*(v2-v1)/(t2-t1)+v1];\n" + "end\n" + "if interval_start>0\n"
        + "\tprune_interval=[prune_interval;[interval_start,interval_end]];\n" + "end\n" + "\n"
        + "hold on,plot(prune_t,prune_v,'b.')\n" + "disp(length(prune_t)/length(t))\n" + "\n"
        + "%rectangle('Position', [at(1) av(1) at(2) av(2)], 'Facec',[0.5 1 0.5])\n"
        + "prune_interval\n" + "\n" + "%for i=1:1:length(prune_interval)\n"
        + "%\txline(prune_interval(i,1));\n" + "%\txline(prune_interval(i,2));\n" + "%end\n" + "\n"
        + "% https://se.mathworks.com/help/matlab/ref/xregion.html\n" + "% xregion\n" + "\n"
        + "for i=1:1:length(prune_interval)\n" + "\tx=prune_interval(i,1):1:prune_interval(i,2);\n"
        + "\ty=[];\n" + "\tfor j=1:1:length(x)\n" + "\t\tidx=x(j);\n" + "\t\tfor n=2:1:length(at)\n"
        + "\t\t\tif idx<=at(n)\n" + "\t\t\t\tt1=at(n-1);\n" + "\t\t\t\tt2=at(n);\n"
        + "\t\t\t\tv1=av(n-1);\n" + "\t\t\t\tv2=av(n);\n"
        + "\t\t\t\ty=[y,(idx-t1)*(v2-v1)/(t2-t1)+v1];\n" + "\t\t\t\tbreak;\n" + "\t\t\tend\n"
        + "\t\tend\n" + "\tend\n" + "\ty1=y-e;\n" + "\ty2=y+e;\n" + "\tL1=[x,fliplr(x)];\n"
        + "\tL2=[y1,fliplr(y2)];\n" + "\tfill(L1,L2,'b','FaceAlpha',0.1);\n" + "end");
  }

  @Test
  public void mytest2() throws Exception {
    String csvData = "D:\\full-game\\BallSpeed.csv";
    int start = 200000;
    int range = 200;
    int end = start + range;
    SDTEncoder encoder = new SDTEncoder();
    double e = 80000.0 / 2; // std/2
    encoder.setCompDeviation(e / 2);
    long count = 0;
    String line;
    List<Integer> timestampList = new ArrayList<>(); // idx
    List<Long> valueList = new ArrayList<>();
    List<Integer> selectTimestamps = new ArrayList<>();
    List<Long> selectValues = new ArrayList<>();
    int idx = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(csvData))) {
      while ((line = reader.readLine()) != null) {
        count++;
        if (count >= start && count < end) {
          idx++;
//          long time = Long.parseLong(line.split(",")[0]);
          int time = idx;
          long value = Long.parseLong(line.split(",")[1]);
          timestampList.add(time);
          valueList.add(value);
          if (encoder.encodeLong(time, value)) {
            selectTimestamps.add((int) encoder.getTime());
            selectValues.add(encoder.getLongValue());
          }
          if (count == end - 1) { // last point
            selectTimestamps.add(time);
            selectValues.add(value);
          }
        } else if (count >= end) {
          break;
        }
      }
    }

    System.out.println("close all;clear all;clc;");
    System.out.println("e=" + e);
    System.out.println("t=" + timestampList + ";");
    System.out.println("v=" + valueList + ";");
    System.out.println("at=" + selectTimestamps + ";");
    System.out.println("av=" + selectValues + ";");

    // 计算maxLB
    long maxVal = Collections.max(selectValues);
//    int maxIdx = selectTimestamps.get(selectValues.indexOf(maxVal));
    double threshold = maxVal - e; // maxLB
    System.out.println("threshold(maxLB)=" + threshold);

    // 计算UB<maxLB的剪枝段
    List<Integer> prune_intervals_start = new ArrayList<>();
    List<Integer> prune_intervals_end = new ArrayList<>();
    int interval_start = -1;
    int interval_end = -1;
    for (int i = 1; i < selectTimestamps.size(); i++) {
      int t1 = selectTimestamps.get(i - 1);
      int t2 = selectTimestamps.get(i);
      double v1 = selectValues.get(i - 1) + e; // UB
      double v2 = selectValues.get(i) + e; // UB
      if (v1 < threshold && v2 < threshold) {
        if (interval_start < 0) {
          interval_start = t1;
        }
        interval_end = t2; // continuous
      } else if (v1 < threshold && v2 >= threshold) {
        if (interval_start < 0) {
          interval_start = t1;
        }
        prune_intervals_start.add(interval_start);
        prune_intervals_end.add((int) Math.floor((threshold - v1) * (t2 - t1) / (v2 - v1) + t1));
        interval_start = -1; // discontinuous
      } else if (v1 >= threshold && v2 < threshold) {
        interval_start = (int) Math.ceil((threshold - v1) * (t2 - t1) / (v2 - v1) + t1);
        interval_end = t2; // continuous
      }
    }
    if (interval_start > 0) {
      prune_intervals_start.add(interval_start);
      prune_intervals_end.add(interval_end);
    }
    System.out.println("UB<maxLB prune intervals (included ends):");
    System.out.println(prune_intervals_start);
    System.out.println(prune_intervals_end);

    // 对剪枝段取余后搜索遍历计算TP
    int startIdx = 1;
    int endIdx = 200;
    startIdx = startIdx - 1; // as excluded treated
    endIdx = endIdx + 1; // as excluded treated
    prune_intervals_start.add(endIdx); // turn into search_intervals_end (excluded)
    prune_intervals_end.add(0, startIdx); // turn into search_intervals_start (excluded)
    long candidateTPvalue = -1;
    int candidateTPidx = -1;
    for (int i = 0; i < prune_intervals_start.size(); i++) {
      int search_interval_start = prune_intervals_end.get(i) + 1; // included
      int search_interval_end = prune_intervals_start.get(i) - 1; // included
      System.out.println(search_interval_start + "," + search_interval_end);
      for (int j = search_interval_start; j <= search_interval_end; j++) {
        long v = valueList.get(j);
        if (v > candidateTPvalue) {
          candidateTPvalue = v;
          candidateTPidx = j;
        }
      }
    }
    System.out.println("TP=(" + candidateTPidx + "," + candidateTPvalue + ")");
  }

  @Test
  public void testLongSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      long value = (long) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeLong(time, value)) {
        count++;
      }
    }
    assertEquals(22, count);
  }

  @Test
  public void testFloatSingleValue() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    int count = 0;
    for (long time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      float value = (float) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      if (encoder.encodeFloat(time, value)) {
        count++;
      }
    }
    assertEquals(14, count);
  }

  @Test
  public void testIntValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    int[] values = new int[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      int value = (int) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(22, size);
  }

  @Test
  public void testDoubleValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    double[] values = new double[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      double value = (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(14, size);
  }

  @Test
  public void testLongValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    long[] values = new long[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      long value = (long) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(22, size);
  }

  @Test
  public void testFloatValueArray() {
    SDTEncoder encoder = new SDTEncoder();
    encoder.setCompDeviation(0.01);

    int degree = 0;
    long[] timestamps = new long[100];
    float[] values = new float[100];

    for (int time = 0; time < 100; time++) {
      // generate data in sine wave pattern
      float value = (float) (10 * Math.sin(degree++ * 3.141592653589793D / 180.0D));
      timestamps[time] = time;
      values[time] = value;
    }
    int size = encoder.encode(timestamps, values, timestamps.length);

    assertEquals(14, size);
  }
}
