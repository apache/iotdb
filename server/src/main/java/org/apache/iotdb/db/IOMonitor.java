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
package org.apache.iotdb.db;

import java.util.ArrayList;
import java.util.List;

/** @author Yuyuan Kang */
public class IOMonitor {

  public static long metaIOTime;
  public static int metaIONum;
  public static long dataIOTime;
  public static int dataIONum;
  public static long readMemChunkTime;
  public static int readMemChunkNum;

  public static long totalTime;
  public static String sql;

  public static List<Long> metaIOTimes = new ArrayList<>();
  public static List<Long> dataIOTimes = new ArrayList<>();
  public static List<Long> readMemChunkTimes = new ArrayList<>();
  public static List<Integer> metaIONums = new ArrayList<>();
  public static List<Integer> dataIONums = new ArrayList<>();
  public static List<Integer> readMemChunkNums = new ArrayList<>();

  public static List<String> sqls = new ArrayList<>();
  public static List<Long> totalTimes = new ArrayList<>();

  public static boolean isSet = false;

  public static void incReadMemChunkTime(long v) {
    readMemChunkTime += v;
    readMemChunkNum++;
  }

  public static void resetReadMemChunkTime() {
    readMemChunkNums.add(readMemChunkNum);
    readMemChunkTimes.add(readMemChunkTime);
    readMemChunkTime = 0;
    readMemChunkNum = 0;
  }

  public static void incMeta(long v) {
    metaIOTime += v;
    metaIONum++;
  }

  private static void resetMeta() {
    metaIOTimes.add(metaIOTime);
    metaIONums.add(metaIONum);
    metaIOTime = 0;
    metaIONum = 0;
  }

  public static void incDataIOTime(long v) {
    dataIOTime += v;
    dataIONum++;
  }

  public static void incTotalTime(long val) {
    totalTime += val;
  }

  private static void resetDataIOTime() {
    dataIOTimes.add(dataIOTime);
    dataIONums.add(dataIONum);
    dataIOTime = 0;
    dataIONum = 0;
  }

  public static void resetTotalTime() {
    totalTimes.add(totalTime);
    totalTime = 0;
  }

  public static void setSQL(String v) {
    if (!isSet) {
      clear();
      isSet = true;
      sql = v;
    } else {
      reset();
      sql = v;
    }
  }

  public static void reset() {
    resetMeta();
    resetDataIOTime();
    resetTotalTime();
    resetReadMemChunkTime();
    sqls.add(sql);
    sql = null;
  }

  //  private static double getAvg(List<Long> vals) {
  //    return (double) getSum(vals) / vals.size();
  //  }

  private static long getSumLong(List<Long> vals) {
    long sum = 0;
    for (Object v : vals) {
      sum += (long) v;
    }
    return sum;
  }

  private static long getSumInteger(List<Integer> vals) {
    Integer sum = 0;
    for (Integer v : vals) {
      sum += v;
    }
    return sum.longValue();
  }

  public static void clear() {
    isSet = false;
    metaIOTime = 0L;
    dataIOTime = 0L;
    totalTime = 0L;
    readMemChunkTime = 0L;
    dataIONum = 0;
    metaIONum = 0;
    readMemChunkNum = 0;
    sql = null;

    metaIOTimes.clear();
    metaIONums.clear();
    dataIONums.clear();
    dataIOTimes.clear();
    sqls.clear();
    totalTimes.clear();
  }

  public static void finish() {
    clear();
    isSet = false;
  }

  public static String print() {
    if (sql != null) {
      reset();
    }
    String ret = "";
    for (int i = 0; i < sqls.size(); i++) {
      ret +=
          sqls.get(i)
              + "\t meta IO: \t"
              + metaIOTimes.get(i)
              + "\t meta num: \t"
              + metaIONums.get(i)
              + "\t data IO: \t"
              + dataIOTimes.get(i)
              + "\t data num: \t"
              + dataIONums.get(i)
              + "\t readMemChunk IO: \t"
              + readMemChunkTimes.get(i)
              + "\t readMemChunk num: \t"
              + readMemChunkNums.get(i)
              + "\t total: \t"
              + totalTimes.get(i)
              + "\n";
    }
    ret +=
        "sum meta IO: \t"
            + getSumLong(metaIOTimes)
            + "\t sum meta nums: \t"
            + getSumInteger(metaIONums)
            + "\t sum data IO: \t"
            + getSumLong(dataIOTimes)
            + "\t sum data num: \t"
            + getSumInteger(dataIONums)
            + "\t sum readMemChunkTime: \t"
            + getSumLong(readMemChunkTimes)
            + "\t sum readMemChunkNum: \t"
            + getSumInteger(readMemChunkNums)
            + "\t avg total time: \t"
            + getSumLong(totalTimes)
            + "\t isSet: \t"
            + isSet;
    return ret;
  }
}
