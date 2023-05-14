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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class StatMedianKLLOverlapIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int baseSize = 7989 * 8; // 7989 * (12518 - 1);
  private static final int TABLET_SIZE = 7989 * 2;
  private static final int device_num = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;

  @BeforeClass
  public static void setUp() throws Exception {
    for (int i = 0; i < device_num; i++) {
      deviceList.add("root.extreme.d" + i);
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
    if (inMemory) {}
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  static final long real_data_series_base_time = 1L << 32;

  private void testTime() throws IoTDBConnectionException, StatementExecutionException {
    int TEST_CASE = 32;
    String[] seriesList = new String[] {"s0"};
    int[] series_len = new int[] {1240};
    String device = "root.extreme.d0";
    List<String> aggrList = new ArrayList<>();
    aggrList.add("exact_median_kll_stat_single");
    aggrList.add("exact_median_kll_stat_overlap_single");
    aggrList.add("count");

    long START_TIME;
    SessionDataSet dataSet;
    String[] result_str = new String[15];
    double[][] result_t = new double[23][23];
    int[][] result_cnt = new int[23][23];
    //    int LEN=180000000;
    int result_row = 0;
    for (String series : seriesList) {
      int series_id = Integer.parseInt(series.substring(1));
      long base_time = 0;
      int series_N = series_len[series_id] * 10000;
      result_row = 0;
      for (int ratio = 10; ratio <= 10; ratio *= 10)
        for (int LEN = (int) 1e6 * ratio; LEN < (int) 1e6 * ratio + 1; LEN += (int) 3e5 * ratio) {
          if (LEN >= series_N) break;
          if (result_str[result_row] == null) result_str[result_row] = "\tLEN:\t" + LEN;
          System.out.print("\t\t\t" + LEN);
          long[] LL = new long[TEST_CASE];
          long[] RR = new long[TEST_CASE];
          for (int i = 0; i < TEST_CASE; i++) {
            LL[i] = base_time + new Random().nextInt(series_N - LEN);
            RR[i] = LL[i] + LEN;
            LL[i] -= LL[i] % (7989 * 2);
            RR[i] -= RR[i] % (7989 * 2);
            RR[i]++;
            //            System.out.println("\t\t\t"+(LL[i]-base_time)+"  "+(RR[i]-base_time));
          }
          double[] relativeDelta = new double[2];
          int[] relativeCnt = new int[2];
          for (int aggrID = 0; aggrID < aggrList.size(); aggrID++) {
            String aggr = aggrList.get(aggrID);
            //        System.out.println(" [aggr] " + aggr);

            long t1 = System.currentTimeMillis(), tmpT;
            long[] times = new long[TEST_CASE];
            for (int i = 0; i < TEST_CASE; i++) {
              //            int series_id = i/(TEST_CASE/seriesList.length);
              //            String series = seriesList[series_id];
              t1 = System.currentTimeMillis();
              dataSet =
                  session.executeQueryStatement(
                      "select "
                          + aggr
                          + "("
                          + series
                          + ")"
                          + " from "
                          + device
                          + " where time >= "
                          + LL[i]
                          + " and time < "
                          + RR[i]);
              tmpT = System.currentTimeMillis();
              times[i] = tmpT - t1;
              String str = dataSet.next().getFields().toString();
              if (aggrID < 2) { // single iteration approx
                double result = Double.parseDouble(str.substring(1, str.length() - 1));
                dataSet =
                    session.executeQueryStatement(
                        "select "
                            + "count"
                            + "("
                            + series
                            + ")"
                            + " from "
                            + device
                            + " where "
                            + series
                            + " <= "
                            + result
                            + " and time >= "
                            + LL[i]
                            + " and time < "
                            + RR[i]);
                String str2 = dataSet.next().getFields().toString();
                double result2 = Double.parseDouble(str2.substring(1, str2.length() - 1));

                dataSet =
                    session.executeQueryStatement(
                        "select "
                            + "count"
                            + "("
                            + series
                            + ")"
                            + " from "
                            + device
                            + " where time >= "
                            + LL[i]
                            + " and time < "
                            + RR[i]);
                String str3 = dataSet.next().getFields().toString();
                long actual_n = Long.parseLong(str3.substring(1, str3.length() - 1));
                double tmpDelta = Math.abs(actual_n / 2.0 - result2) / actual_n;
                //
                // System.out.println("[debug]\t\t\t"+aggr+"\t\t"+result2+"\t\t"+actual_n/2);
                relativeDelta[aggrID] += tmpDelta;
                relativeCnt[aggrID]++;
              }
            }
            Arrays.sort(times);
            int cnt_cases = 0;
            tmpT = 0;
            for (int i = TEST_CASE / 4; i < TEST_CASE * 3 / 4; /* 0; i < TEST_CASE;*/ i++) {
              cnt_cases++;
              tmpT += times[i];
            }
            tmpT /= cnt_cases;
            System.out.print("\t" + tmpT);
            //            result_str[result_row]+="\t"+ tmpT;
            result_t[result_row][aggrID] += tmpT;
            result_cnt[result_row][aggrID]++;
          }
          result_row++;
          System.out.println();
          for (int i = 0; i < 2; i++)
            System.out.println(
                "\t\t" + aggrList.get(i) + ":\t\t" + relativeDelta[i] / relativeCnt[i]);
          for (int i = 0; i < 2; i++) System.out.print("\t" + relativeDelta[i] / relativeCnt[i]);
          System.out.println();
        }
    }
    System.out.print("\t\t");
    for (String str : aggrList) System.out.print("\t" + str);
    System.out.println();

    for (int i = 0; i < result_row; i++)
      for (int j = 0; j < aggrList.size(); j++)
        result_str[i] += "\t" + result_t[i][j] / result_cnt[i][j];
    for (String str : result_str) if (str != null) System.out.println(str);
  }

  private static void prepareTimeSeriesData(double overlap_rate)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    //    System.out.println("\t\t????" + deviceList + "||||" + seriesList);

    long START_TIME = System.currentTimeMillis();
    final int START_SERIES = 0;
    for (String device : deviceList) {
      for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
        String series = seriesList.get(seriesID);
        session.createTimeseries(
            device + "." + series,
            dataTypeList.get(seriesID),
            TSEncoding.PLAIN,
            CompressionType.SNAPPY);
      }
    }

    List<MeasurementSchema> schemaList = new ArrayList<>();

    for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
      schemaList.add(
          new MeasurementSchema(
              seriesList.get(seriesID), dataTypeList.get(seriesID), TSEncoding.PLAIN));
    }

    Random random = new Random(233);
    long REVERSE_TIME = 1L << 40;

    for (int deviceID = 0; deviceID < device_num; deviceID++) {
      String device = deviceList.get(deviceID);
      int TABLET_NUM = (baseSize / TABLET_SIZE) * (deviceID + 1);
      long TOTAL_SIZE = baseSize * (deviceID + 1);
      for (int i = 0; i < TABLET_NUM; i++) {
        long BASE_TIME = i * TABLET_SIZE;
        //        if (i == 0) BASE_TIME = REVERSE_TIME;
        //        else BASE_TIME = (long) (i - 1) * TABLET_SIZE;

        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < TABLET_SIZE; time++) {
          int row = tablet.rowSize++;
          timestamps[row] = BASE_TIME + time;
          long index = timestamps[row];
          if (i > 0 && index / 7989 % 2 == 1 && index % 7989 < 7989 * overlap_rate)
            timestamps[row] -= 7989 * overlap_rate;

          for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
            String series = seriesList.get(seriesID);

            if (seriesID == 0) {
              double num = random.nextGaussian();
              ((double[]) values[seriesID])[row] = num;
            } else if (seriesID == 1) {
              long num = 1; // = ((random.nextInt() & 1) == 1) ? 1 : -1;
              num = num * (long) (Math.pow(10, 1 + random.nextDouble() * 17.5)); // iid log-uniform
              ((double[]) values[seriesID])[row] = Double.longBitsToDouble(num);
            } else if (seriesID == 2) {
              double num = index;
              ((double[]) values[seriesID])[row] = num;
            } else if (seriesID == 3) {
              double num = (index % 7989) * Math.sin(index % 7989);
              ((double[]) values[seriesID])[row] = num;
            }
          }
        }
        session.insertTablet(tablet);
        session.executeNonQueryStatement("flush");
      }
    }
    System.out.println(
        "\t\t create designed data cost time:" + (System.currentTimeMillis() - START_TIME));
  }

  @Test
  public void executeStatement()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    SessionDataSet dataSet;
    dataSet = session.executeQueryStatement("show timeseries");
    while (dataSet.hasNext()) System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    //        dataSet = session.executeQueryStatement("select count(*) from root.**");
    //    while (dataSet.hasNext()) System.out.println("[DEBUG]" +
    // dataSet.next().getFields().toString());
    //    dataSet =
    //        session.executeQueryStatement(
    //            "select exact_median_kll_stat_single(s0)" + " from
    // root.construct_stat_disorder.d0");
    //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    long ST;
    //    ST = new Date().getTime();
    //    for (int i = 0; i < 1; i++)
    //      dataSet = session.executeQueryStatement("select count(s0) from root.disorder_noBF.d0");
    //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));

    ST = new Date().getTime();
    for (int i = 0; i < 1; i++)
      dataSet =
          session.executeQueryStatement(
              "select exact_median_kll_stat_single(s0) from root.disorder_0.d0");
    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    System.out.println("\t\ttime:" + (new Date().getTime() - ST));

    //    ST = new Date().getTime();
    //    for (int i = 0; i < 20; i++)
    //      dataSet =
    //          session.executeQueryStatement(
    //              "select exact_median_kll_stat_single(s0) from root.disorder_8.d0");
    //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
    //
    //    ST = new Date().getTime();
    //    for (int i = 0; i < 20; i++)
    //      dataSet =
    //          session.executeQueryStatement(
    //              "select exact_median_kll_stat_single(s0) from root.disorder_12.d0");
    //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
    //
    //    ST = new Date().getTime();
    //    for (int i = 0; i < 20; i++)
    //      dataSet =
    //          session.executeQueryStatement(
    //              "select exact_median_kll_stat_single(s0) from root.disorder_16.d0");
    //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
    //
    //    ST = new Date().getTime();
    //    for (int i = 0; i < 20; i++)
    //      dataSet = session.executeQueryStatement("select count(s0) from root.disorder_0.d0");
    //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
    //
    //    ST = new Date().getTime();
    //    for (int i = 0; i < 20; i++)
    //      dataSet = session.executeQueryStatement("select count(s0) from root.disorder_16.d0");
    //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
  }

  @Test
  public void run() throws IoTDBConnectionException, StatementExecutionException, IOException {
    int TEST_CASE = 16;
    SessionDataSet dataSet = null;
    long ST;
    String aggr = "count"; // exact_median_kll_stat_single
    String MiuSig = "21";
    double sig = Double.parseDouble(MiuSig.substring(1));
    //    if (MiuSig.length() == 3) sig /= 10;
    System.out.println("Aggr:" + aggr + "\tMiuSig:" + 2 + " " + sig + "\nBF\t" + aggr);
    for (String str : new String[] {"0", "8", "12", "16", "20"}) {

      LongArrayList times = new LongArrayList(TEST_CASE);
      for (int i = 0; i < TEST_CASE; i++) {
        ST = new Date().getTime();
        dataSet =
            session.executeQueryStatement(
                "select " + aggr + "(s0) from root.disorder_" + str + "_" + MiuSig + ".d0");
        times.add(new Date().getTime() - ST);
      }
      times.sortThis();
      long totT = 0, cntT = 0;
      for (int i = TEST_CASE / 4; i < TEST_CASE * 3 / 4; i++) {
        totT += times.get(i);
        cntT++;
      }
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      System.out.println(str + "\t" + totT / cntT);
    }
  }
}
