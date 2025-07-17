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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class DupliQuantile_CompareBaselinesIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int TABLET_SIZE = 8192;
  private static final int baseSize = 5010000; // (30 * 30 * 30 - 1);
  private static final int deviceNumL = 0, deviceNumR = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;
  private static final long RANDOM_SEED = new Date().getTime();

  @BeforeClass
  public static void setUp() throws Exception {
    for (int i = deviceNumL; i < deviceNumR; i++) {
      deviceList.add("root.test.d" + i);
      sizeList.add(baseSize * (i + 1));
    }
    for (int i = 0; i < series_num; i++) {
      seriesList.add("s" + i);
      if (i < Long_Series_Num) dataTypeList.add(TSDataType.INT64);
      else dataTypeList.add(TSDataType.DOUBLE);
    }
    originCompactionThreadNum = CONFIG.getConcurrentCompactionThread();
    CONFIG.setConcurrentCompactionThread(0);
    if (inMemory) EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();
    if (inMemory) {
      // prepareTimeSeriesData();
      // insertDataFromTXT(5);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  DecimalFormat formatT = new DecimalFormat("#0.000");
  DecimalFormat formatPass = new DecimalFormat("#0.00");
  DecimalFormat formatRelative = new DecimalFormat("#0.00000");

  Object2DoubleOpenHashMap<Pair<String, Integer>> datasetMem2Alpha =
      new Object2DoubleOpenHashMap<>();
  Object2DoubleOpenHashMap<String> dataset2MinV = new Object2DoubleOpenHashMap<>();

  void prepareParamsForDD() {
    int[] VoltageDDAlphaPow95For16KBTo256KB =
        new int[] {104, 117, 126, 135, 140, 144, 147, 150, 153, 155, 157, 159, 161, 163, 164, 166};
    for (int i = 16, j = 0; i <= 256; i += 16, j++)
      datasetMem2Alpha.put(
          Pair.of("voltage", i), Math.pow(0.95, VoltageDDAlphaPow95For16KBTo256KB[j]));
    int[] PriceDDAlphaPow95For16KBTo256KB =
        new int[] {100, 113, 121, 129, 134, 138, 141, 144, 147, 150, 152, 154, 156, 157, 159, 161};
    for (int i = 16, j = 0; i <= 256; i += 16, j++)
      datasetMem2Alpha.put(Pair.of("price", i), Math.pow(0.95, PriceDDAlphaPow95For16KBTo256KB[j]));
    int[] CustomDDAlphaPow95For16KBTo256KB =
        new int[] {100, 104, 108, 117, 121, 126, 127, 130, 134, 135, 139, 140, 141, 143, 144, 147};
    for (int i = 16, j = 0; i <= 256; i += 16, j++)
      datasetMem2Alpha.put(
          Pair.of("custom", i), Math.pow(0.95, CustomDDAlphaPow95For16KBTo256KB[j]));
    for (int alpha = 1; alpha <= 14; alpha++) {
      datasetMem2Alpha.put(Pair.of("zipf" + alpha, 64), Math.pow(0.95, 105));
      datasetMem2Alpha.put(Pair.of("zipf" + alpha, 128), Math.pow(0.95, 130));
    }
    for (int i = 2; i <= 20; i += 2) {
      datasetMem2Alpha.put(Pair.of("lognormal" + i, 64), Math.pow(0.95, 130));
      datasetMem2Alpha.put(Pair.of("lognormal" + i, 128), Math.pow(0.95, 147));
    }
    dataset2MinV.put("voltage", -205.25);
    dataset2MinV.put("price", 0);
    dataset2MinV.put("custom", 0);
    for (int alpha = 1; alpha <= 14; alpha++) dataset2MinV.put("zipf" + alpha, 1.0);
    for (int i = 2; i <= 20; i += 2) dataset2MinV.put("lognormal" + i, 1.0);
  }

  String getParamsForDD(String dataset, int memKB) {
    return ", 'dd_alpha'="
        + "'"
        + datasetMem2Alpha.getDouble(Pair.of(dataset, memKB))
        + "'"
        + ", 'min_v'="
        + "'"
        + dataset2MinV.getDouble(dataset)
        + "'";
  }

  @Test
  public void varyingN() throws IoTDBConnectionException, StatementExecutionException, IOException {
    prepareParamsForDD();
    long ALL_T = -new Date().getTime();
    int DROP_METHOD = 1, startType = 0, endType = 2;
    int[] TESTCASE_DATASET = new int[] {10, 10, 5};
    int[] TESTCASE_METHOD = new int[] {1, 1, 1, 1, 1, 1, 1};
    String[] methods =
        new String[] {
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_dd",
          "dupli_quantile_dss",
          "dupli_quantile_gk",
          "dupli_quantile_td",
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_kll_pair"
        };
    String[] xLegendName =
        new String[] {"Dropped", "DD", "DSS", "GK", "TD", "KLL-vanilla", "KLL-dupli"};

    String[] datasets =
        new String[] {
          "voltage", "price", "custom",
        };
    double[] NSmall = new double[] {1E6, 2E6, 4E6, 6E6, 8E6, 10E6, 12E6, 14E6, 16E6, 18E6, 20E6};
    double[][] Ns = new double[][] {NSmall, NSmall, NSmall};
    int NsLength = Ns[0].length;
    double[] MaxNs =
        new double[] {
          3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7, 3e7
        };
    int[] memoryKB =
        new int[] {
          128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128
        };
    // int[] memoryKB = new int[] {32, 32, 32, 32, 32, 32}; // for relative
    // findPrTime
    double[] results = new double[datasets.length * NsLength * methods.length];
    double[] Ts = new double[datasets.length * NsLength * methods.length];
    SessionDataSet dataSet;

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int nid = 0; nid < Ns[datasetID].length; nid++) {
        for (int methodID = 0; methodID < methods.length; methodID++) {
          for (int qid = 0; qid < TESTCASE_DATASET[datasetID] * TESTCASE_METHOD[methodID]; qid++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * qid);

            String sql = "select " + methods[methodID] + "(s0";
            double q = randomQ.nextDouble();
            sql += ",'quantile'='" + q + "'";
            sql += ",'memory'='" + memoryKB[datasetID] + "KB'";
            if (methods[methodID].contains("dd"))
              sql += getParamsForDD(datasets[datasetID], memoryKB[datasetID]);
            sql += ") from root." + datasets[datasetID] + ".d0";

            int n = (int) Ns[datasetID][nid],
                timeL = randomQ.nextInt((int) MaxNs[datasetID] - n),
                timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            // System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * NsLength) + nid) * methods.length + methodID;
            results[id] += result;
            Ts[id] += tmpT;
          }
        }
      }
    }
    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      System.out.println(
          "--------------\n"
              + "dataset:\t"
              + datasets[datasetID]
              + "\n\tMemory:\t"
              + memoryKB[datasetID]
              + "KB"
              + "\nX-axis:\tN"
              + "\tY-axis:\tQuery time(s)");
      System.out.print("N\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int nid = 0; nid < Ns[datasetID].length; nid++) {
        System.out.print(Ns[datasetID][nid] + "\t" + (nid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = Ts[((datasetID * NsLength) + nid) * methods.length + methodID];
          System.out.print(
              "\t"
                  + formatT.format(
                      tmp / TESTCASE_DATASET[datasetID] / TESTCASE_METHOD[methodID] / 1000.0));
        }
        System.out.println("");
      }
    }
    ALL_T += new Date().getTime();
    System.out.println("\nALL_T:" + ALL_T / 1000 + "s");
  }

  @Test
  public void varyingMemory()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    prepareParamsForDD();
    long ALL_T = -new Date().getTime();
    int DROP_METHOD = 1, startType = 0, endType = 2;
    int[] TESTCASE_DATASET = new int[] {8, 8, 3};
    int[] TESTCASE_METHOD = new int[] {1, 1, 1, 1, 1, 1, 1};
    String[] methods =
        new String[] {
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_dd",
          "dupli_quantile_dss",
          "dupli_quantile_gk",
          "dupli_quantile_td",
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_kll_pair"
        };
    String[] xLegendName =
        new String[] {"Dropped", "DD", "DSS", "GK", "TD", "KLL-vanilla", "KLL-dupli"};

    String[] datasets = new String[] {"voltage", "price", "custom"};
    int[] MSmall =
        new int[] {16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256};
    int[] MMedian = new int[] {};
    int[] MLarge = new int[] {};
    int[][] Ms = new int[][] {MSmall, MSmall, MSmall};

    // int[] MRelative =
    // new int[] {64, 96, 128, 160, 192, 224, 256, 320, 384, 448, 512 /*, 768,
    // 1024*/};
    // int[][] Ms =
    // new int[][] {
    // MRelative, MRelative, MRelative, MRelative, MRelative, MRelative
    // }; // for relative findPrTime
    int MsLength = Math.max(Math.max(MSmall.length, MLarge.length), MMedian.length);
    double[] MaxNs = new double[] {3e7, 3e7, 3e7};
    double[] Ns = new double[] {1e7, 1e7, 1e7};
    SessionDataSet dataSet;
    double[] results = new double[datasets.length * MsLength * methods.length];
    double[] Ts = new double[datasets.length * MsLength * methods.length];

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int mid = 0; mid < Ms[datasetID].length; mid++) {

        for (int methodID = 0; methodID < methods.length; methodID++) {
          for (int qid = 0; qid < TESTCASE_DATASET[datasetID] * TESTCASE_METHOD[methodID]; qid++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * qid);

            String sql = "select " + methods[methodID] + "(s0";
            double q = randomQ.nextDouble();
            sql += ",'quantile'='" + q + "'";
            sql += ",'memory'='" + Ms[datasetID][mid] + "KB'";
            if (methods[methodID].contains("dd"))
              sql += getParamsForDD(datasets[datasetID], Ms[datasetID][mid]);
            sql += ") from root." + datasets[datasetID] + ".d0";

            int n = (int) Ns[datasetID],
                timeL = randomQ.nextInt((int) MaxNs[datasetID] - n),
                timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            // System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * MsLength) + mid) * methods.length + methodID;
            results[id] += result;
            Ts[id] += tmpT;
          }
        }
      }
    }

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      System.out.println(
          "--------------\n"
              + "dataset:\t"
              + datasets[datasetID]
              + "\n\tN:\t"
              + Ns[datasetID]
              + "\nX-axis:\tMemory(KB)"
              + "\tY-axis:\tQuery time(s)");
      System.out.print("Memory(KB)\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int mid = 0; mid < Ms[datasetID].length; mid++) {
        System.out.print(Ms[datasetID][mid] + "\t" + (mid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = Ts[((datasetID * MsLength) + mid) * methods.length + methodID];
          System.out.print(
              "\t"
                  + formatT.format(
                      tmp / TESTCASE_DATASET[datasetID] / TESTCASE_METHOD[methodID] / 1000.0));
        }
        System.out.println("");
      }
    }
    ALL_T += new Date().getTime();
    System.out.println("\nALL_T:" + ALL_T / 1000 + "s");
  }

  @Test
  public void varyingZipf()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    prepareParamsForDD();
    long ALL_T = -new Date().getTime();
    int DROP_METHOD = 1, startType = 0, endType = 13, TEST_CASE_BASE = 10;
    int[] TESTCASE_DATASET =
        new int[] {
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE,
          TEST_CASE_BASE
        };
    int[] TESTCASE_METHOD = new int[] {1, 1, 1, 1, 1, 1, 1};
    String[] methods =
        new String[] {
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_dd",
          "dupli_quantile_dss", // 2min per dss 12s others
          "dupli_quantile_gk",
          "dupli_quantile_td",
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_kll_pair"
        };
    String[] xLegendName =
        new String[] {"Dropped", "DD", "DSS", "GK", "TD", "KLL-vanilla", "KLL-dupli"};

    String[] datasets =
        new String[] {
          "zipf1", "zipf2", "zipf3", "zipf4", "zipf5", "zipf6", "zipf7", "zipf8", "zipf9", "zipf10",
          "zipf11", "zipf12", "zipf13", "zipf14",
        };
    double N = 1e7;
    double MaxN = 3e7;
    int memoryKB = 64;
    double[] results = new double[datasets.length * 1 * methods.length];
    double[] Ts = new double[datasets.length * 1 * methods.length];
    SessionDataSet dataSet;

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int nid = 0; nid < 1; nid++) {
        for (int methodID = 0; methodID < methods.length; methodID++) {
          for (int qid = 0; qid < TESTCASE_DATASET[datasetID] * TESTCASE_METHOD[methodID]; qid++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * qid);

            String sql = "select " + methods[methodID] + "(s0";
            double q = randomQ.nextDouble();
            sql += ",'quantile'='" + q + "'";
            sql += ",'memory'='" + memoryKB + "KB'";
            if (methods[methodID].contains("dd"))
              sql += getParamsForDD(datasets[datasetID], memoryKB);
            sql += ") from root." + datasets[datasetID] + ".d0";

            int n = (int) N, timeL = randomQ.nextInt((int) MaxN - n), timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            // System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * 1) + nid) * methods.length + methodID;
            results[id] += result;
            Ts[id] += tmpT;
          }
        }
      }
    }
    System.out.print("\t\t\t\t\t\t\t");
    for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
      System.out.print("\t" + xLegendName[methodID]);
    System.out.println("");
    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      // System.out.println(
      // "--------------\n"
      // + "dataset:\t"
      // + datasets[datasetID]
      // + "\n\tMemory:\t"
      // + memoryKB[datasetID]
      // + "KB"
      // + "\nX-axis:\tN"
      // + "\tY-axis:\tQuery time(s)");
      System.out.print(
          "N:\t"
              + N
              + "\tmemKB:\t"
              + memoryKB
              + "Zipf:\t"
              + formatT.format((datasetID + 1) * 0.1)
              + "\t"
              + (datasetID + 1)
              + "\t");
      // for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
      // System.out.print("\t" + xLegendName[methodID]);
      // System.out.println("");
      for (int nid = 0; nid < 1; nid++) {
        System.out.print(N + "\t" + (datasetID + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = Ts[((datasetID * 1) + nid) * methods.length + methodID];
          System.out.print(
              "\t"
                  + formatT.format(
                      tmp / TESTCASE_DATASET[datasetID] / TESTCASE_METHOD[methodID] / 1000.0));
        }
        System.out.println("");
      }
    }
    ALL_T += new Date().getTime();
    System.out.println("\nALL_T:" + ALL_T / 1000 + "s");
  }

  @Test
  public void varyingLognormal()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    prepareParamsForDD();
    long ALL_T = -new Date().getTime();
    int DROP_METHOD = 1, startType = 0, endType = 9, TEST_CASE_BASE = 1;
    int TESTCASE = 10;
    // int[] TESTCASE_METHOD = new int[] {4, 10, 2, 10, 10, 10, 10};
    String[] methods =
        new String[] {
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_dd",
          "dupli_quantile_dss", // 2min per dss 12s others
          "dupli_quantile_gk",
          "dupli_quantile_td",
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_kll_pair"
        };
    String[] xLegendName =
        new String[] {"Dropped", "DD", "DSS", "GK", "TD", "KLL-vanilla", "KLL-dupli"};

    String[] datasets =
        new String[] {
          "lognormal2", "lognormal4", "lognormal6", "lognormal8", "lognormal10",
          "lognormal12", "lognormal14", "lognormal16", "lognormal18", "lognormal20",
        };
    double N = 1e7;
    double MaxN = 3e7;
    int memoryKB = 64;
    double[] results = new double[datasets.length * 1 * methods.length];
    double[] Ts = new double[datasets.length * 1 * methods.length];
    SessionDataSet dataSet;

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int nid = 0; nid < 1; nid++) {
        for (int methodID = 0; methodID < methods.length; methodID++) {
          for (int qid = 0; qid < TESTCASE /* TESTCASE_METHOD[methodID] */; qid++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * qid);

            String sql = "select " + methods[methodID] + "(s0";
            double q = randomQ.nextDouble();
            sql += ",'quantile'='" + q + "'";
            sql += ",'memory'='" + memoryKB + "KB'";
            if (methods[methodID].contains("dd"))
              sql += getParamsForDD(datasets[datasetID], memoryKB);
            sql += ") from root." + datasets[datasetID] + ".d0";

            int n = (int) N, timeL = randomQ.nextInt((int) MaxN - n), timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            // System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * 1) + nid) * methods.length + methodID;
            results[id] += result;
            Ts[id] += tmpT;
          }
        }
      }
    }
    System.out.print("\t\t\t\t\t\t\t");
    for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
      System.out.print("\t" + xLegendName[methodID]);
    System.out.println("");
    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      System.out.print(
          "N:\t"
              + N
              + "\tmemKB:\t"
              + memoryKB
              + "\tLognormal:\t"
              + (datasetID + 1) * 2
              + "\t"
              + (datasetID + 1)
              + "\t");
      // for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
      // System.out.print("\t" + xLegendName[methodID]);
      // System.out.println("");
      for (int nid = 0; nid < 1; nid++) {
        System.out.print(N + "\t" + (datasetID + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = Ts[((datasetID * 1) + nid) * methods.length + methodID];
          System.out.print(
              "\t" + formatT.format(tmp / TESTCASE /* / TESTCASE_METHOD[methodID] */ / 1000.0));
        }
        System.out.println("");
      }
    }
    ALL_T += new Date().getTime();
    System.out.println("\nALL_T:" + ALL_T / 1000 + "s");
  }

  @Test
  public void varyingQueriedQuantile()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    prepareParamsForDD();
    long ALL_T = -new Date().getTime();
    int[] QUANTILE_PER_TEST = new int[] {3, 3, 3};
    int DROP_METHOD = 1, startType = 0, endType = 2;
    // double[] tempQ = new double[] {0.0001,0.001,0.01,0.1,0.5, 0.9, 0.99, 0.999,
    // 0.9999};
    // double[] tempQ =
    // new double[] {
    // 0.0001, 0.01, 0.5,
    // 0.99, 0.9999 /**/
    // };
    // int numPerQ = 3;
    String[] methods =
        new String[] {
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_dd",
          "dupli_quantile_dss", // 2min per dss 12s others
          "dupli_quantile_gk",
          "dupli_quantile_td",
          "dupli_quantile_kll_vanilla",
          "dupli_quantile_kll_pair",
          "dupli_quantile_req"
        };
    String[] xLegendName =
        new String[] {"Dropped", "DD", "DSS", "GK", "TD", "KLL-vanilla", "KLL-dupli", "Req"};

    String[] datasets = new String[] {"voltage", "price", "custom"};
    // DoubleArrayList tempQs = new DoubleArrayList();
    // // for(int
    // //
    // i=0;i<=(tempQ.length-1)*numPerQ;i++)tempQs.add((i%numPerQ==0)?tempQ[i/numPerQ]:tempQ[i/numPerQ]*Math.pow(tempQ[i/numPerQ+1]/tempQ[i/numPerQ],1.0/(numPerQ)*(i%numPerQ)));
    // for (int i = 0; i <= (tempQ.length - 1) * numPerQ; i++)
    // tempQs.add(
    // (i % numPerQ == 0)
    // ? tempQ[i / numPerQ]
    // : tempQ[i / numPerQ]
    // + (tempQ[i / numPerQ + 1] - tempQ[i / numPerQ]) / numPerQ * (i % numPerQ));

    DoubleArrayList tempQs = new DoubleArrayList();
    for (int i = 1; i <= 5; i++) tempQs.add(i / 100.0);
    for (int i = 10; i <= 90; i += 5) tempQs.add(i / 100.0);
    for (int i = 95; i <= 99; i++) tempQs.add(i / 100.0);
    double[] Qs = tempQs.toDoubleArray();
    // System.out.println("\t\t" + tempQs);
    int QsLength = Qs.length;
    double[] MaxNs = new double[] {3e7, 3e7, 3e7};
    double[] Ns = new double[] {1e7, 1e7, 1e7};
    SessionDataSet dataSet;
    int[] Ms = new int[] {128, 128, 128};
    double[] results = new double[datasets.length * QsLength * methods.length];
    double[] Ts = new double[datasets.length * QsLength * methods.length];

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int quantileid = 0; quantileid < Qs.length; quantileid++) {
        for (int qid = 0; qid < QUANTILE_PER_TEST[datasetID]; qid++) {

          for (int methodID = 0; methodID < methods.length; methodID++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * qid);

            String sql = "select " + methods[methodID] + "(s0";
            double q = Qs[quantileid];
            sql += ",'quantile'='" + q + "'";
            sql += ",'memory'='" + Ms[datasetID] + "KB'";
            sql += ",'return_type'='value'";
            if (methods[methodID].contains("dd"))
              sql += getParamsForDD(datasets[datasetID], Ms[datasetID]);
            sql += ") from root." + datasets[datasetID] + ".d0";

            int n = (int) Ns[datasetID],
                timeL = randomQ.nextInt((int) MaxNs[datasetID] - n),
                timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            // System.out.println("\t\tsql:\t" + sql);
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            // System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * QsLength) + quantileid) * methods.length + methodID;
            results[id] += result;
            Ts[id] += tmpT;
          }
        }
      }
    }
    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      System.out.println(
          "--------------\n"
              + "dataset:\t"
              + datasets[datasetID]
              + "\n\tN:\t"
              + Ns[datasetID]
              + "\nX-axis:\tQuantile"
              + "\tY-axis:\tQuery time(s)");
      System.out.print("Quantile\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int quantileid = 0; quantileid < Qs.length; quantileid++) {
        System.out.print(Qs[quantileid] + "\t" + (quantileid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = Ts[((datasetID * QsLength) + quantileid) * methods.length + methodID];
          System.out.print("\t" + formatT.format(tmp / QUANTILE_PER_TEST[datasetID] / 1000.0));
        }
        System.out.println("");
      }
    }
    ALL_T += new Date().getTime();
    System.out.println("\nALL_T:" + ALL_T / 1000 + "s");
  }
}
