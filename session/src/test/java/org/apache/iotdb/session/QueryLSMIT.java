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

package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class QueryLSMIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final int baseSize = 4096 * 5000; // 7989 * (12518 - 1);
  private static final int series_num = 1;
  private static final boolean inMemory = false;
  private static final long REVERSE_TIME = 1L << 60, UPDATE_ARRIVAL_TIME = 1L << 50;
  private static final List<String> storageGroupList = new ArrayList<>();
  private static final int datasetID = 1;
  int TEST_CASE = 512;
  int queryN = 100000000, seriesN = 110000000;

  @BeforeClass
  public static void setUp() throws Exception {

    storageGroupList.add("root.Summary" + "3");
    storageGroupList.add("root.Summary" + "0");
    //    storageGroupList.add("root.noSum");
    originCompactionThreadNum = CONFIG.getConcurrentCompactionThread();
    CONFIG.setConcurrentCompactionThread(0);
    if (inMemory) EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (inMemory) {}
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  private String getQueryStatement(String body, long L, long R) {
    return body + " where time>=" + L + " and time<" + R;
  }

  private void testTime() throws IoTDBConnectionException, StatementExecutionException {
    List<String> aggrList = new ArrayList<>();
    aggrList.add("kll_quantile");
    aggrList.add("count");

    SessionDataSet dataSet;
    //    System.out.println("\t\t\tqueryN:" + queryN + "\tDataset: random");
    long[] LL = new long[TEST_CASE];
    long[] RR = new long[TEST_CASE];
    Random random = new Random(233);
    for (int i = 0; i < TEST_CASE; i++) {
      LL[i] = 0;
      //       random.nextInt(seriesN - queryN + 1);
      RR[i] = LL[i] + queryN;
      //      System.out.println("\t\t\t"+(LL[i])+"  "+(RR[i]));
    }
    for (String sg : storageGroupList) {
      System.out.print(queryN);
      for (String aggr : aggrList) {
        //        if (aggr.equals("count")) if (!sg.contains("Summary0")) continue;
        String queryBody = "select " + aggr + "(" + "s0" + ") from " + sg + ".d0";
        session.executeQueryStatement(queryBody);
        for (int i = 0; i < TEST_CASE / 8 + 4; i++)
          session.executeQueryStatement(getQueryStatement(queryBody, LL[i], RR[i]));
        // warm up.

        long TIME = new Date().getTime();
        LongArrayList tList = new LongArrayList();
        for (int t = 0; t < TEST_CASE; t++) {
          dataSet = session.executeQueryStatement(getQueryStatement(queryBody, LL[t], RR[t]));
          long mmp = new Date().getTime();
          tList.add(mmp - TIME);
          TIME = mmp;
          //          System.out.println(getQueryStatement(queryBody,LL[t],RR[t]));
        }
        tList.sort(Long::compare);
        long sum = 0, cnt = 0;
        for (int i = TEST_CASE / 4; i < TEST_CASE * 3 / 4; cnt++, i++) sum += tList.getLong(i);
        System.out.print("\t\t" + 1.0 * sum / cnt);
      }
    }
    System.out.println();
  }

  //  @Test
  //  public void executeStatement()
  //      throws IoTDBConnectionException, StatementExecutionException, IOException {
  //    SessionDataSet dataSet;
  //    dataSet = session.executeQueryStatement("show timeseries");
  //    while (dataSet.hasNext()) System.out.println("[DEBUG]" +
  // dataSet.next().getFields().toString());
  //    long ST;
  //    ST = new Date().getTime();
  //    for (int i = 0; i < 1; i++)
  //      dataSet =
  //          session.executeQueryStatement(
  //              "select exact_median_kll_stat_single(s0) from "
  //                  + storageGroupList.get(0)
  //                  + " where time<"
  //                  + REVERSE_TIME);
  //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
  //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
  //  }

  @Test
  public void run() throws IoTDBConnectionException, StatementExecutionException, IOException {
    long ST = new Date().getTime();
    System.out.println(
        "queryN\t\ttime_lsm\t\ttime_count_lsm\t\ttime_chunk\t\ttime_count_chunk\t\t两个count只是数据不同");
    for (int x = 20000000; x <= 100000000; x += 20000000) {
      queryN = x;
      testTime();
    }
    //    testValue();
    System.out.println("\t\tALL_TIME:" + (new Date().getTime() - ST));
  }
}
