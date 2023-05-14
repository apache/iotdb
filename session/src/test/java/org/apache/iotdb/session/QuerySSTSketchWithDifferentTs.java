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

public class QuerySSTSketchWithDifferentTs {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final boolean inMemory = false;
  int TEST_CASE = 64;
  int queryN = 40000000, seriesN = 8192 * 6713;

  @BeforeClass
  public static void setUp() throws Exception {
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
    String dataset = "thruster";
    List<String> aggrList = new ArrayList<>();
    aggrList.add("kll_quantile");

    System.out.println("\t\t\tqueryN:" + queryN + "\tDataset:" + dataset);
    long[] LL = new long[TEST_CASE];
    long[] RR = new long[TEST_CASE];
    Random random = new Random(233);
    for (int i = 0; i < TEST_CASE; i++) {
      LL[i] = 0; // random.nextInt(seriesN - queryN + 1);
      RR[i] = LL[i] + queryN;
      //      System.out.println("\t\t\t"+(LL[i])+"  "+(RR[i]));
    }
    long ALL_START = new Date().getTime();
    System.out.println();
    SessionDataSet tmpResult;
    for (int T : new int[] {1, 2, 4, 8, 16, 32}) {
      String sgName = "root." + dataset + "4096" + "T" + T;
      String queryBody = "select " + "kll_quantile" + "(s0)" + " from " + sgName + ".d0";

      for (int i = 0; i < TEST_CASE / 8 + 4; i++) session.executeQueryStatement(queryBody); // drop

      long TIME = new Date().getTime();
      LongArrayList tArr = new LongArrayList();
      for (int t = 0; t < TEST_CASE; t++) {
        long tmpT = new Date().getTime();
        tmpResult =
            session.executeQueryStatement(/*getQueryStatement(queryBody, LL[t], RR[t])*/ queryBody);
        tmpT = new Date().getTime() - tmpT;
        tArr.add(tmpT);
        //          System.out.println(getQueryStatement(queryBody,LL[t],RR[t]));
      }
      tArr.sort(Long::compare);

      System.out.print(T + "\t\t" + 1.0 * tArr.getLong(tArr.size() / 2) + "\t");
      TIME = new Date().getTime() - TIME;
      System.out.print("" + 1.0 * TIME / TEST_CASE + "\n");
    }

    System.out.println(
        "\n\n\t\tTEST_CASE=" + TEST_CASE + "\t\tALL_TIME=" + (new Date().getTime() - ALL_START));
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
    testTime();
    //    testValue();
  }
}
