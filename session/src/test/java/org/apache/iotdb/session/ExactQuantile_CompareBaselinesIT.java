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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class ExactQuantile_CompareBaselinesIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int TABLET_SIZE = 8192;
  private static final int baseSize = 5010000; // (30 * 30  * 30  - 1);
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
      //      prepareTimeSeriesData();
      //      insertDataFromTXT(5);
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

  @Test
  public void varyingN() throws IoTDBConnectionException, StatementExecutionException, IOException {
    long ALL_T = -new Date().getTime();
    int DROP_METHOD = 1, startType = 0, endType = 5;
    String useAnyStat = ",'useAnyStat'='true'",
        returnType = ",'return_type'='findPrTime'"; // ",'return_type'='iteration_num'";
    int[] QUANTILE_PER_TEST = new int[] {300, 300, 300, 300, 300, 300, 40}; // 25s,8min,
    String[] methods =
        new String[] {
          "exact_quantile_quick_select",
          //                              "exact_quantile_quick_select",
          //                    "exact_quantile_gk",
          //                    "exact_quantile_tdigest",
          //                    "exact_quantile_ddsketch_positive",
          "exact_quantile_pr_kll_post_best_pr",
          //                    "exact_quantile_pr_kll_post_best_pr",
          //                    "exact_quantile_pr_kll_post_best_pr",
          //                    "exact_quantile_pr_kll_post_best_pr",
          //                    "exact_quantile_pr_kll_post_best_pr",
          //                    "exact_quantile_pr_kll_post_best_pr",
        };
    String[] xLegendName =
        new String[] {
          "Dropped",
          //                              "QuickSelect",
          //                    "GKSketch",
          //                    "t-digest",
          //                    "DDSketch",
          "δ*-KLL",
          //          "NoPre-δ*-KLL",
          //          "0.1%-KLL",
          //          "0.5%-KLL",
          //          "1%-KLL",
          //          "5%-KLL",
        };
    String[] params =
        new String[] {
          "",
          //                              "",
          //                              "",
          //                    ",'param'='1'",
          //                    "",
          ",'merge_buffer_ratio'='5'",
          //          ",'merge_buffer_ratio'='0'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.001'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.005'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.01'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.05'",

          //          "", // for NotUseStat
          //          "",
          //          "",
          //          ",'param'='1'",
          //          "",
          //          ",'merge_buffer_ratio'='0'",
        };
    int synopsisByte = 512;
    String[] deviceSuffix =
        new String[] {
          "",
          //                              "",
          //                              "",
          //                    synopsisByte + "ByteTD",
          //                    synopsisByte + "ByteDD",
          synopsisByte + "ByteKLL",
          //          "",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",

          //                    "", // for NotUseStat
          //                    "",
          //                    "",
          //                    "",
          //                    "",
          //                    "",
        };

    String[] datasets =
        new String[] {
          "bitcoin", "thruster", "electric", "binance", "ibm", "Synthetic", "full_binance"
        };
    double[] NSmall =
        new double[] {5E5, 1E6, 1E6 + 5E5, 2E6, 2E6 + 5E5, 3E6, 3E6 + 5E5, 4E6, 4E6 + 5E5, 5E6};
    double[] NMedian =
        new double[] {
          /*1e7, 2e7, 3e7, 4e7, */
          5e7, 6e7, 7e7, 8e7, 9e7, 1e8, 1.1e8, 1.2e8, 1.3e8, 1.4e8, 1.5e8 /**/
        };
    double[] NLarge =
        new double[] {
          //                    1e8,
          2e8, 3e8, 4e8,
          //            5e8,
          //            6e8,
          //            7e8,
          //          8e8,
          //          9e8, 1e9 /**/
        };
    //    double[][] Ns = new double[][] {NSmall, NSmall, NSmall, NMedian, NMedian, NLarge, NLarge};
    double[] NRelative =
        new double[] {1e5, 4e5, 7e5, 1e6, 2e6, 3e6, 4e6, 5e6, 6e6, 7e6, 8e6, 9e6, 1e7};
    double[][] Ns =
        new double[][] {
          NRelative, NRelative, NRelative, NRelative, NRelative, NRelative
        }; // for relative findPrTime
    int NsLength = Math.max(Math.max(Ns[0].length, NMedian.length), NLarge.length);
    double[] MaxNs = new double[] {2e7, 2e7, 2e7, 1.77e8, 1.77e8, 1e9 + 1e7, 8.9e8};
    //        int[] memoryKB = new int[] {32, 32, 32, 256, 256, 1024, 1024};
    int[] memoryKB = new int[] {32, 32, 32, 32, 32, 32}; // for relative findPrTime
    double[] results = new double[datasets.length * NsLength * methods.length];
    double[] Ts = new double[datasets.length * NsLength * methods.length];
    SessionDataSet dataSet;

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int nid = 0; nid < Ns[datasetID].length; nid++) {
        for (int qid = 0; qid < QUANTILE_PER_TEST[datasetID]; qid++) {

          for (int methodID = 0; methodID < methods.length; methodID++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * qid);

            String sql = "select " + methods[methodID] + "(s0";
            double q = randomQ.nextDouble();
            sql += ",'quantile'='" + q + "'";
            sql += ",'memory'='" + memoryKB[datasetID] + "KB'";
            sql += params[methodID] + useAnyStat;
            sql += returnType;
            sql += ") from root." + datasets[datasetID] + deviceSuffix[methodID] + ".d0";

            int n = (int) Ns[datasetID][nid],
                timeL = randomQ.nextInt((int) MaxNs[datasetID] - n),
                timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            //            System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * NsLength) + nid) * methods.length + methodID;
            results[id] += returnType.contains("findPrTime") ? result / 1000.0 / tmpT : result;
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
              + "\tY-axis:\t"
              + (returnType.contains("findPr") ? "relativeFindPrTime" : "Passes"));
      System.out.print("N\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int nid = 0; nid < Ns[datasetID].length; nid++) {
        System.out.print(Ns[datasetID][nid] + "\t" + (nid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = results[((datasetID * NsLength) + nid) * methods.length + methodID];
          System.out.print(
              "\t"
                  + (returnType.contains("findPr") ? formatRelative : formatPass)
                      .format(tmp / QUANTILE_PER_TEST[datasetID]));
        }
        System.out.println("");
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
          System.out.print("\t" + formatT.format(tmp / QUANTILE_PER_TEST[datasetID] / 1000.0));
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
    long ALL_T = -new Date().getTime();
    int QUANTILE_PER_TEST = 300;
    int DROP_METHOD = 1, startType = 0, endType = 5; // ?&3min
    String returnType = ",'return_type'='findPrTime'"; // ",'return_type'='iteration_num'";
    String[] methods =
        new String[] {
          "exact_quantile_quick_select",
          //          "exact_quantile_quick_select",
          //                    "exact_quantile_gk",
          //                              "exact_quantile_tdigest",
          //                    "exact_quantile_ddsketch_positive",
          "exact_quantile_pr_kll_post_best_pr",
          //          "exact_quantile_pr_kll_post_best_pr",
          //          "exact_quantile_pr_kll_post_best_pr",
          //          "exact_quantile_pr_kll_post_best_pr",
          //          "exact_quantile_pr_kll_post_best_pr",
          //          "exact_quantile_pr_kll_post_best_pr",
        };
    String[] xLegendName =
        new String[] {
          "Dropped",
          //          "QuickSelect",
          //                    "GKSketch",
          //                              "t-digest",
          //                    "DDSketch",
          "δ*-KLL",
          //          "NoPre-δ*-KLL",
          //          "0.1%-KLL",
          //          "0.5%-KLL",
          //          "1%-KLL",
          //          "5%-KLL",
        };
    String[] params =
        new String[] {
          "",
          // "",
          // "",
          // ",'param'='1'",
          // "",
          ",'merge_buffer_ratio'='5'",
          //          ",'merge_buffer_ratio'='0'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.001'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.005'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.01'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.05'",
        };
    int synopsisByte = 512;
    String[] deviceSuffix =
        new String[] {
          "",
          // "",
          // "",
          // synopsisByte + "ByteTD",
          // synopsisByte + "ByteDD",
          synopsisByte + "ByteKLL",
          //          "",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
        };

    String[] datasets =
        new String[] {"bitcoin", "thruster", "electric", "binance", "ibm", "Synthetic"};
    int[] MSmall =
        new int[] {
          /*16, 20, 24, 28, 32, 40, 48, 56, 64, 80, 96, 112, */ 128,
          1024,
          2048,
          4096,
          8192,
          32768,
          65536,
          131072,
          262144,
          524288,
          1048576,
          2000000
        };
    int[] MMedian =
        new int[] {
          //        32, 64, 96, 128, 256, 384, 512, 640, 768, 896, 1024
          //        64,80,96,112,128,160,192,224,256,320,384,448,512
          /*64, 96, 128, 160, 192, 224,  256, 320, 384, */ 448, 512, 768, 1024
        };
    int[] MLarge =
        new int[] {
          //        128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024
          //        64,96,128,192,256,384,512,768,1024,1536,2048
          /*64, 96,
          128,  160, 192, 224, 256, 320, /* 384, 448, 512, 768, 1024, 1536, */ 2048,
          32768,
          65536,
          131072,
          262144,
          524288,
          1048576,
          2000000
        };
    //    int[][] Ms = new int[][] {MSmall, MSmall, MSmall, MMedian, MMedian, MLarge};

    int[] MRelative =
        new int[] {64, 96, 128, 160, 192, 224, 256, 320, 384, 448, 512 /*, 768, 1024*/};
    int[][] Ms =
        new int[][] {
          MRelative, MRelative, MRelative, MRelative, MRelative, MRelative
        }; // for relative findPrTime
    int MsLength =
        Math.max(
            Math.max(Math.max(MSmall.length, MLarge.length), MMedian.length), MRelative.length);
    double[] MaxNs = new double[] {2e7, 2e7, 2e7, 1.77e8, 1.77e8, 1e9 + 1e7};
    //    double[] Ns = new double[] {4e6, 4e6, 4e6, 1e8, 1e8, 1e8};
    double[] Ns = new double[] {5e6, 5e6, 5e6, 5e6, 5e6, 5e6}; // for relative findPrTime
    SessionDataSet dataSet;
    double[] results = new double[datasets.length * MsLength * methods.length];
    double[] Ts = new double[datasets.length * MsLength * methods.length];

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int mid = 0; mid < Ms[datasetID].length; mid++) {
        for (int qid = 0; qid < QUANTILE_PER_TEST; qid++) {

          for (int methodID = 0; methodID < methods.length; methodID++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * qid);

            String sql = "select " + methods[methodID] + "(s0";
            double q = randomQ.nextDouble();
            sql += ",'quantile'='" + q + "'";
            sql += ",'memory'='" + Ms[datasetID][mid] + "KB'";
            sql += params[methodID];
            sql += returnType;
            sql += ") from root." + datasets[datasetID] + deviceSuffix[methodID] + ".d0";

            int n = (int) Ns[datasetID],
                timeL = randomQ.nextInt((int) MaxNs[datasetID] - n),
                timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            //            System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * MsLength) + mid) * methods.length + methodID;
            results[id] += returnType.contains("findPrTime") ? result / 1000.0 / tmpT : result;
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
              + "\tY-axis:\t"
              + (returnType.contains("findPr") ? "relativeFindPrTime" : "Passes"));
      System.out.print("Memory(KB)\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int mid = 0; mid < Ms[datasetID].length; mid++) {
        System.out.print(Ms[datasetID][mid] + "\t" + (mid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = results[((datasetID * MsLength) + mid) * methods.length + methodID];
          System.out.print(
              "\t"
                  + (returnType.contains("findPr") ? formatRelative : formatPass)
                      .format(tmp / QUANTILE_PER_TEST));
        }
        System.out.println("");
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
          System.out.print("\t" + formatT.format(tmp / QUANTILE_PER_TEST / 1000.0));
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
    long ALL_T = -new Date().getTime();
    int[] QUANTILE_PER_TEST = new int[] {1000, 150, 150, 100, 1, 900}; // 24s&10min
    int DROP_METHOD = 0, startType = 6, endType = 5;
    //    double[] tempQ = new double[] {0.0001,0.001,0.01,0.1,0.5, 0.9, 0.99, 0.999, 0.9999};
    double[] tempQ =
        new double[] {
          /*0.0001, 0.01, 0.5, */
          0.99, 0.9999 /**/
        };
    int numPerQ = 3;
    //  double[] tempQ = new double[]{0.0001,0.0034000000000000002,0.0067};
    //    int numPerQ = 1;
    String[] methods =
        new String[] {
          //                    "exact_quantile_quick_select",
          "exact_quantile_quick_select",
          //          "exact_quantile_gk",
          //          "exact_quantile_tdigest",
          //          "exact_quantile_ddsketch_positive",
          "exact_quantile_pr_kll_post_best_pr",
        };
    String[] xLegendName =
        new String[] {
          //                    "Dropped",
          "QuickSelect",
          //          "GKSketch",
          //          "t-digest",
          //            "DDSketch",
          "δ*-KLL",
          //          "NoPre-δ*-KLL",
          //          "0.1%-KLL",
          //          "0.5%-KLL",
          //          "1%-KLL",
          //          "5%-KLL",
        };
    String[] params =
        new String[] {
          //                    "",
          "",
          //          "",
          //          ",'param'='1'",
          //            "",
          ",'merge_buffer_ratio'='5'",
        };
    int synopsisByte = 512;
    String[] deviceSuffix =
        new String[] {
          //                    "",
          "",
          //          "",
          //          synopsisByte + "ByteTD",
          //            synopsisByte + "ByteDD",
          synopsisByte + "ByteKLL",
          //          "",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
        };

    String[] datasets =
        new String[] {"bitcoin", "thruster", "electric", "binance", "ibm", "Synthetic"};
    DoubleArrayList tempQs = new DoubleArrayList();
    //    for(int
    // i=0;i<=(tempQ.length-1)*numPerQ;i++)tempQs.add((i%numPerQ==0)?tempQ[i/numPerQ]:tempQ[i/numPerQ]*Math.pow(tempQ[i/numPerQ+1]/tempQ[i/numPerQ],1.0/(numPerQ)*(i%numPerQ)));
    for (int i = 0; i <= (tempQ.length - 1) * numPerQ; i++)
      tempQs.add(
          (i % numPerQ == 0)
              ? tempQ[i / numPerQ]
              : tempQ[i / numPerQ]
                  + (tempQ[i / numPerQ + 1] - tempQ[i / numPerQ]) / numPerQ * (i % numPerQ));
    double[] Qs = tempQs.toDoubleArray();
    //    System.out.println("\t\t" + tempQs);
    int QsLength = Qs.length;
    double[] MaxNs = new double[] {2e7, 2e7, 2e7, 1.77e8, 1.77e8, 1e9 + 1e7};
    double[] Ns = new double[] {4e6, 4e6, 4e6, 1e8, 1e8, 1e8};
    SessionDataSet dataSet;
    int[] Ms = new int[] {32, 32, 32, 256, 256, 256};
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
            sql += params[methodID];
            sql += ",'return_type'='iteration_num'";
            sql += ") from root." + datasets[datasetID] + deviceSuffix[methodID] + ".d0";

            int n = (int) Ns[datasetID],
                timeL = randomQ.nextInt((int) MaxNs[datasetID] - n),
                timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            long tmpT = -new Date().getTime();
            //            System.out.println("\t\tsql:\t" + sql);
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            //            System.out.println("\tresult:\t" + result + "\t\t" + sql);
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
              + "\tY-axis:\tPasses");
      System.out.print("Quantile\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int quantileid = 0; quantileid < Qs.length; quantileid++) {
        System.out.print(Qs[quantileid] + "\t" + (quantileid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = results[((datasetID * QsLength) + quantileid) * methods.length + methodID];
          System.out.print("\t" + formatPass.format(tmp / QUANTILE_PER_TEST[datasetID]));
        }
        System.out.println("");
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

  @Test
  public void varyingQuantity()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    long ALL_T = -new Date().getTime();
    int DROP_METHOD = 0, startType = 6, endType = 5;
    boolean KTimeMemory = true;
    int[] TEST_CASE = new int[] {50, 50, 50, 10, 10, 20}; // 1&40min   0.5&9min(sharedPass)
    String[] methods =
        new String[] {
          //                    "exact_multi_quantiles_quick_select",
          //                              "exact_multi_quantiles_quick_select",
          //                              "exact_multi_quantiles_gk",
          //                    "exact_multi_quantiles_tdigest",
          "exact_multi_quantiles_ddsketch_positive",
          //          "exact_multi_quantiles_pr_kll_post_best_pr",
          // "exact_multi_quantiles_pr_kll_post_best_pr",
          //          "exact_multi_quantiles_pr_kll_post_best_pr",
          //          "exact_multi_quantiles_pr_kll_post_best_pr",
          //          "exact_multi_quantiles_pr_kll_post_best_pr",
          //          "exact_multi_quantiles_pr_kll_post_best_pr",
        };
    String[] xLegendName =
        new String[] {
          //                    "Dropped",
          //                              "QuickSelect",
          //                              "GKSketch",
          //                    "t-digest",
          "DDSketch", "δ*-KLL",
          //          "NoPre-δ*-KLL",
          //          "0.1%-KLL",
          //          "0.5%-KLL",
          //          "1%-KLL",
          //          "5%-KLL",
          //          "No shared pass", // for no shared pass
          //          "shared pass",
        };
    String[] params =
        new String[] {
          //                    "",
          //                              "",
          //                              "",
          //                              ",'param'='1'",
          "", ",'merge_buffer_ratio'='5'",
          //          ",'merge_buffer_ratio'='0'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.001'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.005'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.01'",
          //          ",'merge_buffer_ratio'='5','fix_delta'='0.05'",

          //            "", // for K times memory
          //            "",
          //            "",
          //            ",'param'='1'",
          //          "",
          //            ",'merge_buffer_ratio'='0'",
          //          ",'merge_buffer_ratio'='5'" + ",'sharedPass'='false'", // for no shared pass
          //          ",'merge_buffer_ratio'='5'" + ",'sharedPass'='true'",
        };
    int synopsisByte = 512;
    String[] deviceSuffix =
        new String[] {
          //                    "",
          //                              "",
          //                              "",
          //                    synopsisByte + "ByteTD",
          synopsisByte + "ByteDD", synopsisByte + "ByteKLL",
          //          "",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          synopsisByte + "ByteKLL",
          //          "", // for K times memory
          //          "",
          //          "",
          //          "",
          //          "",
          //          "",
          //          synopsisByte + "ByteKLL", // for no shared pass
          //          synopsisByte + "ByteKLL",
        };

    String[] datasets =
        new String[] {"bitcoin", "thruster", "electric", "binance", "ibm", "Synthetic"};
    int[] QSmall =
        new int[] {
          //            1, 2, 4, 7, 10, 13, 16, 30, 44, 58, 72,86, 100 // 512KB
          8, 12, 16, 20, 24, 28, 32, 48, 64, 80, 96, 112, 128 /**/
          //            1,2,4,8,16,32,64,128,256
          //                      2, 33, 67, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000 /**/
          //          2, 20,40,60,80, 100, 150, 200, 250, 300, 350, 400, 450, 500 // for no shared
          // pass
          //            51
        };
    int[] QLarge =
        new int[] {
          /*2, 33, 67, 100, 200, 300, 400, */ 500, /* 600, //700, 800, 900, 1000 /**/
          //                      2, 1000 /**/
          //          2, 20,40,60,80, 100, 150, 200, 250, 300, 350, 400, 450, 500 // for no shared
          // pass
          //            51
        };
    int[][] Qs = new int[][] {QSmall, QSmall, QSmall, QLarge, QLarge, QLarge};
    int QsLength = Math.max(QSmall.length, QLarge.length);
    double[] MaxNs = new double[] {2e7, 2e7, 2e7, 1.77e8, 1.77e8, 1e9 + 1e7};
    double[] Ns = new double[] {4e6, 4e6, 4e6, 1e8, 1e8, 1e8};
    int[] Ms = new int[] {1024, 1024, 1024, 8192, 8192, 8192};
    int[] MsForKTimes = new int[] {8, 8, 8, 16, 16, 16}; // new int[] {32, 32, 32, 256, 256, 256};
    SessionDataSet dataSet;
    double[] results = new double[datasets.length * QsLength * methods.length];
    double[] Ts = new double[datasets.length * QsLength * methods.length];

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      for (int qid = 0; qid < Qs[datasetID].length; qid++) {
        for (int testID = 0; testID < TEST_CASE[datasetID]; testID++) {

          for (int methodID = 0; methodID < methods.length; methodID++) {
            Random randomQ = new Random(RANDOM_SEED + 1000 * testID);

            String sql = "select " + methods[methodID] + "(s0";
            sql += ",'multi_quantiles'='" + Qs[datasetID][qid] + "'";
            sql +=
                ",'memory'='"
                    + (!KTimeMemory ? Ms[datasetID] : MsForKTimes[datasetID] * Qs[datasetID][qid])
                    + "KB'";
            sql += params[methodID];
            sql += ",'return_type'='iteration_num'";
            sql += ") from root." + datasets[datasetID] + deviceSuffix[methodID] + ".d0";

            int n = (int) Ns[datasetID],
                timeL = randomQ.nextInt((int) MaxNs[datasetID] - n),
                timeR = timeL + n;
            sql += " where time>=" + timeL + " and time<" + timeR;
            //                        System.out.println(sql);
            long tmpT = -new Date().getTime();
            dataSet = session.executeQueryStatement(sql);
            tmpT += new Date().getTime();
            String tmp = dataSet.next().getFields().toString();
            double result = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));

            //            System.out.println("\tresult:\t" + result + "\t\t" + sql);
            int id = ((datasetID * QsLength) + qid) * methods.length + methodID;
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
              + (!KTimeMemory ? "\tMemory:\t" : "\tBaseMemory:\t")
              + (!KTimeMemory ? Ms[datasetID] : MsForKTimes[datasetID])
              + "KB"
              + "\nX-axis:\tQuantile num"
              + "\tY-axis:\tPasses");
      System.out.print("Quantile num\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int qid = 0; qid < Qs[datasetID].length; qid++) {
        System.out.print(Qs[datasetID][qid] + "\t" + (qid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = results[((datasetID * QsLength) + qid) * methods.length + methodID];
          System.out.print("\t" + formatPass.format(tmp / TEST_CASE[datasetID]));
        }
        System.out.println("");
      }
    }

    for (int datasetID = startType; datasetID <= endType; datasetID++) {
      System.out.println(
          "--------------\n"
              + "dataset:\t"
              + datasets[datasetID]
              + "\n\tN:\t"
              + Ns[datasetID]
              + (!KTimeMemory ? "\tMemory:\t" : "\tBaseMemory:\t")
              + (!KTimeMemory ? Ms[datasetID] : MsForKTimes[datasetID])
              + "KB"
              + "\nX-axis:\tQuantile num"
              + "\tY-axis:\tQuery time(s)");
      System.out.print("Quantile num\t");
      for (int methodID = DROP_METHOD; methodID < methods.length; methodID++)
        System.out.print("\t" + xLegendName[methodID]);
      System.out.println("");
      for (int qid = 0; qid < Qs[datasetID].length; qid++) {
        System.out.print(Qs[datasetID][qid] + "\t" + (qid + 1));
        for (int methodID = DROP_METHOD; methodID < methods.length; methodID++) {
          double tmp = Ts[((datasetID * QsLength) + qid) * methods.length + methodID];
          System.out.print("\t" + formatT.format(tmp / TEST_CASE[datasetID] / 1000.0));
        }
        System.out.println("");
      }
    }
    ALL_T += new Date().getTime();
    System.out.println("\nALL_T:" + ALL_T / 1000 + "s");
  }
}
