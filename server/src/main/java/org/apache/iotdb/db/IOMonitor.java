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
  public static long totalTime;
  public static String sql;

  public static List<Long> metaIOTimes = new ArrayList<>();
  public static List<Long> dataIOTimes = new ArrayList<>();
  public static List<Integer> metaIONums = new ArrayList<>();
  public static List<Integer> dataIONums = new ArrayList<>();

  public static List<String> sqls = new ArrayList<>();
  public static List<Long> totalTimes = new ArrayList<>();

  public static boolean isSet = false;

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

  private static void resetDataIOTime() {
    dataIOTimes.add(dataIOTime);
    dataIONums.add(dataIONum);
    dataIOTime = 0;
    dataIONum = 0;
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
    sqls.add(sql);
    sql = null;
  }

  private static double getAvg(List<Long> vals) {
    long sum = 0;
    for (long v : vals) {
      sum += v;
    }
    return (double) sum / vals.size();
  }

  public static void clear() {
    isSet = false;
    metaIOTime = 0L;
    dataIOTime = 0L;
    totalTime = 0L;
    dataIONum = 0;
    metaIONum = 0;
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
    reset();
    String ret = "";
    for (int i = 0; i < sqls.size(); i++) {
      ret +=
          sqls.get(i)
              + "\t meta IO: "
              + metaIOTimes.get(i)
              + "\t meta num: "
              + metaIONums.get(i)
              + "\t data IO: "
              + dataIOTimes.get(i)
              + "\t data num: "
              + dataIONums.get(i)
              + "\t total: "
              + totalTimes.get(i);
      //              + "\n";
    }
    //    ret +=
    //        "avg meta IO: "
    //            + getAvg(metaIOTimes)
    //            + ", avg data IO: "
    //            + getAvg(dataIOTimes)
    //            + ", avg total time: "
    //            + getAvg(totalTimes)
    //            + ", isSet: "
    //            + isSet;
    return ret;
  }
}
