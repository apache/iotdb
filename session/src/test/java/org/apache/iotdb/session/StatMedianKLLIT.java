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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.fail;

public class StatMedianKLLIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int baseSize = 100000000;
  private static final int TABLET_SIZE = 200000;
  private static final int device_num = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 4;
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
    if (inMemory) {
      prepareTimeSeriesData();
      //      insertDataFromTXT(5);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  @Test
  public void complexAggregationTest() {
    try {
      SessionDataSet dataSet;
      dataSet =
          session.executeQueryStatement("select exact_median_kll_stat(s0) from root.extreme.d0");
      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      dataSet =
          session.executeQueryStatement(
              "select exact_median_kll_stat_debug_full_reading(s0) from root.extreme.d0");
      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      dataSet =
          session.executeQueryStatement("select exact_median_kll_floats(s0) from root.extreme.d0");
      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      dataSet =
          session.executeQueryStatement(
              "select exact_median_kll_debug_full_reading(s0) from root.extreme.d0");
      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareTimeSeriesData()
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

    for (int deviceID = 0; deviceID < device_num; deviceID++) {
      String device = deviceList.get(deviceID);
      int TABLET_NUM = (baseSize / TABLET_SIZE) * (deviceID + 1);
      long TOTAL_SIZE = baseSize * (deviceID + 1);
      for (int i = 0; i < TABLET_NUM; i++) {
        long BASE_TIME = (long) i * TABLET_SIZE;
        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < TABLET_SIZE; time++) {
          int row = tablet.rowSize++;
          timestamps[row] = BASE_TIME + time;
          long index = timestamps[row];

          for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
            String series = seriesList.get(seriesID);

            if (seriesID == 0) {
              long num = 1; // = ((random.nextInt() & 1) == 1) ? 1 : -1;
              num = num * (long) (Math.pow(10, 1 + random.nextDouble() * 17.5)); // iid log-uniform
              ((double[]) values[seriesID])[row] = Double.longBitsToDouble(num);
            } else if (seriesID == 1) {
              double num = index;
              ((double[]) values[seriesID])[row] = num;
            } else if (seriesID == 2) {
              double num = random.nextDouble();
              ((double[]) values[seriesID])[row] = num;
            } else if (seriesID == 3) {
              double num = (index % 7989) * Math.sin(index % 7989);
              ((double[]) values[seriesID])[row] = num;
            }
          }
        }
        session.insertTablet(tablet);
      }
    }
    System.out.println(
        "\t\t create designed data cost time:" + (System.currentTimeMillis() - START_TIME));
  }

  static final long real_data_series_base_time = 1L << 32;

  private void testRealTime() throws IoTDBConnectionException, StatementExecutionException {
    int TEST_CASE = 12;
    //    String[] seriesList = new String[] {"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"};
    //    int[] series_len = new int[] {55, 55, 55, 55, 60, 131, 131, 356};

    String[] seriesList = new String[] {"s0"};
    int[] series_len = new int[] {55};
    String device = "root.real_8192.d0";
    List<String> aggrList = new ArrayList<>();
    //    aggrList.add("exact_median_kll_floats");
    //    aggrList.add("exact_median_kll_stat_single_read");
    //    aggrList.add("exact_median_kll_floats");
    //    aggrList.add("exact_median_kll_stat_debug_full_reading");
    //    aggrList.add("exact_median_kll_debug_full_reading");
    //    aggrList.add("exact_median_kll_stat_debug_page_demand_rate");
    aggrList.add("count");
    aggrList.add("count");
    aggrList.add("exact_median_kll_stat");
    aggrList.add("exact_median_kll_stat_single");
    aggrList.add("exact_median_kll_floats");
    aggrList.add("exact_median_kll_floats_single");
    //    aggrList.add("exact_median_kll_stat_single");
    //    aggrList.add("exact_median_kll_stat_single_read");
    //    aggrList.add("exact_median_kll_stat_single");
    //    aggrList.add("exact_median_kll_stat");

    System.out.print("\t\tLEN");
    for (String aggr : aggrList) System.out.print("\t" + aggr);
    System.out.println();

    long START_TIME;
    SessionDataSet dataSet;
    String[] result_str = new String[15];
    double[][] result_t = new double[23][23];
    int[][] result_cnt = new int[23][23];
    //    int LEN=180000000;
    int result_row = 0;
    for (String series : seriesList) {
      int series_id = Integer.parseInt(series.substring(1));
      long base_time = series_id * real_data_series_base_time;
      int series_N = series_len[series_id] * 1000000;
      result_row = 0;
      for (int ratio = 100; ratio <= 100; ratio *= 10)
        for (int LEN = (int) 1e5 * ratio; LEN < (int) 2e5 * ratio; LEN += (int) 3e5 * ratio) {
          if (LEN >= series_N) break;
          if (result_str[result_row] == null) result_str[result_row] = "\tLEN:\t" + LEN;
          System.out.print("\t\t\t" + LEN);
          long[] LL = new long[TEST_CASE];
          long[] RR = new long[TEST_CASE];
          for (int i = 0; i < TEST_CASE; i++) {
            LL[i] = base_time + new Random().nextInt(series_N - LEN);
            RR[i] = LL[i] + LEN;
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
              //              String str = dataSet.next().getFields().toString();
              //              if (aggr.contains("_debug")) System.out.print(str);
              //            if (aggrID==0) { // single iteration approx
              //              //            System.out.println("\t\t" + str);
              //              double result = Double.parseDouble(str.substring(1, str.length() -
              // 1));
              //              dataSet =
              //                  session.executeQueryStatement(
              //                      "select "
              //                          + "count"
              //                          + "("
              //                          + series
              //                          + ")"
              //                          + " from "
              //                          + device
              //                          + " where "
              //                          + series
              //                          + " <= "
              //                          + result
              //                          + " and time >= "
              //                          + LL[i]
              //                          + " and time < "
              //                          + RR[i]);
              //              String str2 = dataSet.next().getFields().toString();
              //              double result2 = Double.parseDouble(str2.substring(1, str2.length() -
              // 1));
              //
              //              double tmpDelta = Math.abs(LEN / 2.0 - result2) / LEN;
              //              //            System.out.println("\t\t\t"+result2+"\t\t"+tmpDelta);
              //              relativeDelta[aggrID] += tmpDelta;
              //              relativeCnt[aggrID]++;
              //            }
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
          //        System.out.println("\n\t\t"+aggrList.get(0)+
          //          ":" + relativeDelta[0] / relativeCnt[0] + "\n");
        }
    }
    System.out.print("\t");
    for (String str : aggrList) System.out.print("\t" + str);
    System.out.println();

    for (int i = 0; i < result_row; i++)
      for (int j = 0; j < aggrList.size(); j++)
        result_str[i] += "\t" + result_t[i][j] / result_cnt[i][j];
    for (String str : result_str) if (str != null) System.out.println(str);
  }

  private void TXTTest() throws IoTDBConnectionException, StatementExecutionException {
    final int TOT_TXT = 5;
    List<String> aggrList = new ArrayList<>();
    aggrList.add("avg");
    aggrList.add("exact_median");
    aggrList.add("exact_median_opt_4");
    aggrList.add("exact_median_amortized");
    aggrList.add("exact_median_kll_floats");
    aggrList.add("exact_median_aggressive");
    aggrList.add("exact_median_bits_bucket_stat");
    aggrList.add("exact_median_bits_bucket_stat_filter");
    aggrList.add("exact_median_bits_bucket_stat_filter_aggressive");

    int TEST_CASE = 8;
    long START_TIME;
    SessionDataSet dataSet;
    System.out.print("\t\t\t\t");
    for (int seriesID = 0; seriesID < TOT_TXT; seriesID++) System.out.print("\t" + "s" + seriesID);
    System.out.println();

    String device = "root.real.d0";
    for (String aggr : aggrList) {
      System.out.println(" [aggr] " + aggr);
      System.out.print("\t\t\t\t");
      for (int seriesID = 0; seriesID < TOT_TXT; seriesID++) {
        String series = "s" + seriesID;
        START_TIME = System.currentTimeMillis();
        long t1 = START_TIME, tmpT;
        long[] times = new long[TEST_CASE];
        for (int i = 0; i < TEST_CASE; i++) {
          dataSet =
              session.executeQueryStatement(
                  "select " + aggr + "(" + series + ")" + " from " + device);
          tmpT = System.currentTimeMillis();
          times[i] = tmpT - t1;
          t1 = tmpT;
        }
        Arrays.sort(times);
        int cnt_cases = 0;
        tmpT = 0;
        for (int i = TEST_CASE / 4; i < TEST_CASE * 3 / 4; i++) {
          cnt_cases++;
          tmpT += times[i];
        }
        System.out.print("\t" + tmpT / cnt_cases);
      }
      System.out.println();
    }
  }

  static final int realSeriesNum = 7;

  private static void insertDataFromTXT()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    int sketchSize = 1024;
    String device = "root.real_" + sketchSize + ".d0";
    long START_TIME = new Date().getTime();
    String[] fileList = new String[10];
    fileList[0] = "tmp_1_55.txt";
    fileList[1] = "tmp_3_55.txt";
    fileList[2] = "tmp_0_55.txt";
    fileList[3] = "tmp_2_55.txt";
    fileList[4] = "tmp_1_60.txt";
    fileList[5] = "tmp_0_131.txt";
    fileList[6] = "tmp_1_131.txt";
    fileList[7] = "tmp_0_356.txt";
    for (int i = 0; i < 1; i++) {
      String series = "s" + i;
      session.createTimeseries(
          device + "." + series, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
      MeasurementSchema schema = new MeasurementSchema(series, TSDataType.DOUBLE, TSEncoding.PLAIN);
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(schema);

      Random random = new Random(233);
      String filename = fileList[i];
      String folder = "E:\\real-world data\\Kaggle";
      String filepath = folder + "\\" + filename;
      File file = new File(filepath);
      BufferedInputStream fis = null;
      fis = new BufferedInputStream(new FileInputStream(file));
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8), 50 * 1024 * 1024);
      reader.readLine(); // ignore first line!

      long CNT_TIME = 0;
      String tmps;
      boolean over_flag = false;
      while (!over_flag) {
        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        for (int j = 0; j < TABLET_SIZE; j++) {
          if ((tmps = reader.readLine()) != null) {
            int row = tablet.rowSize++;
            timestamps[row] = CNT_TIME;
            ((double[]) values[0])[row] = Double.parseDouble(tmps);
            CNT_TIME++;
          } else {
            over_flag = true;
            break;
          }
        }
        if (!over_flag) {
          session.insertTablet(tablet);
        }
      }
    }
    System.out.println(
        "\t\t[WRITE FINISH]:\t" + sketchSize + "\t" + (new Date().getTime() - START_TIME));
  }

  @Test
  public void insertDATA() {
    try {
      prepareTimeSeriesData();
      //      insertDataFromTXT();
      //      insertDataFromTXT();
      //      insertDataFromTXT(3, 3, 0);
    } catch (IoTDBConnectionException | StatementExecutionException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void run() throws IoTDBConnectionException, StatementExecutionException, IOException {
    //        prepareTimeSeriesData();
    //            insertDataFromTXT();
    //        complexAggregationTest();
    testRealTime();
  }
}
