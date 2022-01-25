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
  public static long dataIOTime;
  public static long totalTime;
  public static String sql;

  public static List<Long> metaIOTimes = new ArrayList<>();
  public static List<Long> dataIOTimes = new ArrayList<>();
  public static List<String> sqls = new ArrayList<>();
  public static List<Long> totalTimes = new ArrayList<>();

  public static void incMeta(long v) {
    metaIOTime += v;
  }

  private static void resetMeta() {
    metaIOTimes.add(metaIOTime);
    metaIOTime = 0;
  }

  public static void incDataIOTime(long v) {
    dataIOTime += v;
  }

  private static void resetDataIOTime() {
    dataIOTimes.add(dataIOTime);
    dataIOTime = 0;
  }

  public static void setSQL(String v) {
    sql = v;
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
    metaIOTime = 0L;
    dataIOTime = 0L;
    totalTime = 0L;
    sql = null;

    metaIOTimes.clear();
    dataIOTimes.clear();
    sqls.clear();
    totalTimes.clear();
  }

  @Override
  public String toString() {
    return "meta IO: "
        + getAvg(metaIOTimes)
        + ", data IO: "
        + getAvg(dataIOTimes)
        + ", total time: "
        + getAvg(totalTimes);
  }
}
