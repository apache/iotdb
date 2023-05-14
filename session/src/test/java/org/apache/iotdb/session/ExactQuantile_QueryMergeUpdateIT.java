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

public class ExactQuantile_QueryMergeUpdateIT {
  static int TEST_CASE = 1;
  static int startType = 1, endType = 3;
  //  static int startType = 0, endType = 0;
  static int queryN = 5000000;
  //  static int queryN=100000000;
  static String queryKB = "32KB",
      querySyn = "512Byte",
      queryMergeBufferRatio = "0",
      queryMergeUpdate = "false";
  //  static String queryKB="1024KB", querySyn="512Byte", queryMergeBufferRatio="5",
  // queryMergeUpdate="true";
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final int baseSize = (int) (2e7);
  //  private static final int baseSize = (int) (1e8);
  private static final boolean inMemory = false;
  private static final List<String> storageGroupList = new ArrayList<>();
  static List<String> aggrList = new ArrayList<>();

  @BeforeClass
  public static void setUp() throws Exception {
    storageGroupList.add("root.syn");
    storageGroupList.add("root.bitcoin");
    storageGroupList.add("root.thruster");
    storageGroupList.add("root.taxi");

    aggrList.add("exact_quantile_pr_kll_post_best_pr");

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

  private void testTime(int dataType, double updRate)
      throws IoTDBConnectionException, StatementExecutionException {

    String UpdateRateStr = String.valueOf((int) Math.round(updRate * 10000));
    while (UpdateRateStr.length() < 5) UpdateRateStr = "0" + UpdateRateStr;

    SessionDataSet dataSet;
    //    System.out.println("\t\t\tqueryN:" + queryN + "\tDataset: random");
    long[] LL = new long[TEST_CASE];
    long[] RR = new long[TEST_CASE];
    Random random = new Random(233);
    for (int i = 0; i < TEST_CASE; i++) {
      LL[i] = random.nextInt(baseSize - queryN + 1);
      RR[i] = LL[i] + queryN;
      //      System.out.println("\t\t\t"+(LL[i])+"  "+(RR[i]));
    }
    String sg = storageGroupList.get(dataType) + querySyn + UpdateRateStr;

    System.out.print(
        "TEST_CASE="
            + TEST_CASE
            + "\tDATASET:"
            + dataType
            + " "
            + sg
            + "\tqueryN:\t"
            + queryN
            + "\tmemory:\t"
            + queryKB
            + "\tmerge_buffer_ratio:\t"
            + queryMergeBufferRatio
            + "\tmerge_update:\t"
            + queryMergeUpdate
            + "\t\t");

    int aggrSize = aggrList.size();
    double[] resultT = new double[aggrSize];
    double[] resultV = new double[aggrSize];
    for (int aggrID = 0; aggrID < aggrSize; aggrID++) {
      String aggr = aggrList.get(aggrID);
      //        if (aggr.equals("count")) if (!sg.contains("Summary0")) continue;
      String queryBody =
          "select "
              + aggr
              + "("
              + "s0"
              + ",'merge_update'='"
              + queryMergeUpdate
              + "','memory'='"
              + queryKB
              + "','merge_buffer_ratio'='"
              + queryMergeBufferRatio
              + "','return_type'='iteration_num'"
              //                                + ",'return_type'='value'"
              + ") from "
              + sg
              + ".d0";
      //      System.out.println("\t\t??\t\t"+queryBody);

      for (int i = 0; i < TEST_CASE; i++)
        session.executeQueryStatement(getQueryStatement(queryBody, LL[i], RR[i]));
      for (int i = 0; i < TEST_CASE; i++)
        session.executeQueryStatement(getQueryStatement(queryBody, LL[i], RR[i]));

      long TIME = new Date().getTime();
      LongArrayList tList = new LongArrayList();
      for (int t = 0; t < TEST_CASE; t++) {
        dataSet = session.executeQueryStatement(getQueryStatement(queryBody, LL[t], RR[t]));
        String tmpS = dataSet.next().getFields().toString();
        resultV[aggrID] += Double.parseDouble(tmpS.substring(1, tmpS.length() - 1)) / TEST_CASE;
        long mmp = new Date().getTime();
        tList.add(mmp - TIME);
        TIME = mmp;
        //                    System.out.print("\t|"+tmpS+"|\t");
        //          System.out.println(getQueryStatement(queryBody,LL[t],RR[t]));
      }
      tList.sort(Long::compare);
      long sum = 0, cnt = 0;
      //        for (int i = TEST_CASE / 4; i < TEST_CASE * 2 / 4; cnt++, i++) sum +=
      // tList.getLong(i);
      for (int i = 0; i < TEST_CASE; cnt++, i++) sum += tList.getLong(i);
      double avgT = 1.0 * sum / cnt;

      //        System.out.println("\t\t\t\t\tq:" + quantile + "\t\tavgT:" + avgT + "\t\t" +
      // tList);
      resultT[aggrID] += avgT;
    }
    for (int aggrID = 0; aggrID < aggrSize; aggrID++)
      System.out.print("\t\t\tavgT:\t" + resultT[aggrID] + "\tavgV:\t" + resultV[aggrID]);

    System.out.println();
  }

  @Test
  public void run() throws IoTDBConnectionException, StatementExecutionException, IOException {
    long ST = new Date().getTime();
    System.out.println("Query with Merge Update.");

    for (int dataType = startType; dataType <= endType; dataType++) {
      for (double upd : new double[] {0, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5 /**/}) {
        queryMergeBufferRatio = "0";
        queryMergeUpdate = "false";
        querySyn = "0Byte";
        testTime(dataType, upd);
        queryMergeBufferRatio = "5";
        queryMergeUpdate = "true";
        querySyn = "512Byte";
        System.out.print("\t");
        testTime(dataType, upd);
      }
      //      queryMergeBufferRatio="0";queryMergeUpdate="false";
      //      for (double upd : new double[]{0, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5})
      //        testTime(dataType, upd);
      ////      TEST_CASE*=4;
      //      queryMergeBufferRatio="5";queryMergeUpdate="true";
      //      for (double upd : new double[]{0, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5})
      //        testTime(dataType, upd);
      ////      TEST_CASE/=4;
      System.out.println("\n\n");
    }

    System.out.println("\t\tALL_TIME:" + (new Date().getTime() - ST));
  }
}
