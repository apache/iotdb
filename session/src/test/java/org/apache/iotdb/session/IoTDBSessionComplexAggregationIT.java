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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

public class IoTDBSessionComplexAggregationIT {

  //  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int baseSize = 20000000;
  private static final int TABLET_SIZE = 100000;
  private static final int device_num = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 8;
  private static final int Long_Series_Num = 6;
  private static final boolean inMemory = false;

  @BeforeClass
  public static void setUp() throws Exception {
    for (int i = 0; i < device_num; i++) {
      deviceList.add("root.sg1.d" + i);
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
      insertDataFromTXT(5);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  private void testAndCompare() throws IoTDBConnectionException, StatementExecutionException {
    List<String> aggrList = new ArrayList<>();
    //    aggrList.add("avg");
    //    aggrList.add("exact_median");
    //    aggrList.add("exact_median_opt_4");
    //    aggrList.add("exact_median_amortized");
    aggrList.add("exact_median_kll_floats");
    aggrList.add("exact_median_kll_stat");
    aggrList.add("exact_median_aggressive");
    //    aggrList.add("exact_median_bits_bucket_stat");
    //    aggrList.add("exact_median_bits_bucket_stat_filter");
    //    aggrList.add("exact_median_bits_bucket_stat_filter_aggressive");

    int TEST_CASE = 8;
    long START_TIME;
    SessionDataSet dataSet;
    for (int deviceID = 0; deviceID < device_num; deviceID++) {
      System.out.println("\n[RESULT] size:" + sizeList.get(deviceID));

      System.out.print("\t\t\t\t");
      for (String series : seriesList) System.out.print("\t" + series);
      System.out.println();

      String device = deviceList.get(deviceID);
      for (String aggr : aggrList) {
        System.out.println(" [aggr] " + aggr);
        System.out.print("\t\t\t\t");
        for (int seriesID = 0; seriesID < series_num; seriesID++) {
          String series = seriesList.get(seriesID);
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
          //            result[deviceID][seriesID] = System.currentTimeMillis() - START_TIME;
          //          System.out.print("\t" + (System.currentTimeMillis() - START_TIME) /
          // TEST_CASE);
          System.out.print("\t" + tmpT / cnt_cases);
          //                        while (dataSet.hasNext()) {
          //                          RowRecord rowRecord = dataSet.next();
          //                          System.out.println("\t\t\t[DEBUG]" +
          // rowRecord.getFields().toString());
          //                          dataSet.next();
          //                        }
        }
        System.out.println();
      }
    }
  }

  @Test
  public void complexAggregationTest() {
    try {
      long START_TIME;
      int TEST_CASE;
      SessionDataSet dataSet;

      //      dataSet =
      //          session.executeQueryStatement(
      //              "select count(s1), exact_median(s1) from root.sg1.d1");
      //      assertEquals(1, dataSet.getColumnNames().size());
      //      assertEquals("count(" + ROOT_SG1_D1 + ".s1)", dataSet.getColumnNames().get(0));
      //      while (dataSet.hasNext()) {
      //        RowRecord rowRecord = dataSet.next();
      //        assertEquals(2000000, rowRecord.getFields().get(0).getLongV());
      //        System.out.println("[DEBUG]" + rowRecord.getFields().toString());
      //        dataSet.next();
      //      }
      //            SessionDataSet dataSet;
      //            // 10000 3 3 TODO fix bug
      //      dataSet = session.executeQueryStatement("select count(s3) from root.sg1.d0 where s3=0
      // and s3!=-1 and time<1000000");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet = session.executeQueryStatement("select exact_median_opt_3(s3) from
      // root.sg1.d0 where s3!=-1");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //
      //      dataSet = session.executeQueryStatement("select exact_median(s0) from root.sg1.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet =
      //          session.executeQueryStatement("select exact_median_kll_floats(s5) from
      // root.real.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet = session.executeQueryStatement("select exact_median_kll_stat(s5) from
      // root.real.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet = session.executeQueryStatement( "select exact_median_kll_floats_single(s5)
      // from root.real.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet = session.executeQueryStatement( "select exact_median_kll_stat_single(s5) from
      // root.real.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet = session.executeQueryStatement("select count(s7) from root.sg1.d0 where
      // s7<=-1.179151410150348E-200");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet = session.executeQueryStatement("select exact_median_kll_stat(s2) from
      // root.sg1.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet =
      //          session.executeQueryStatement(
      //              "select exact_median_aggressive(s2) from root.sg1.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet =
      //          session.executeQueryStatement(
      //              "select exact_median_opt_4(s2) from root.sg1.d0");
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      dataSet =
      //          session.executeQueryStatement(
      //              "select s2 from root.real.d0 limit 50");
      //      for(int i=0;i<50;i++)
      //      System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
      //      testAndCompare();
      //      TXTTest();
      NewTXTTest();
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private static void prepareTimeSeriesData()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    System.out.println("\t\t????" + deviceList + "||||" + seriesList);
    long START_TIME = System.currentTimeMillis();

    for (String device : deviceList) {
      for (int seriesID = 0; seriesID < series_num; seriesID++) {
        String series = seriesList.get(seriesID);
        session.createTimeseries(
            device + "." + series,
            dataTypeList.get(seriesID),
            TSEncoding.PLAIN,
            CompressionType.SNAPPY);
      }
    }

    List<MeasurementSchema> schemaList = new ArrayList<>();

    for (int seriesID = 0; seriesID < series_num; seriesID++) {
      schemaList.add(
          new MeasurementSchema(
              seriesList.get(seriesID), dataTypeList.get(seriesID), TSEncoding.PLAIN));
    }
    //    for (String series : seriesList)
    //      schemaList.add(new MeasurementSchema(series, TSDataType.INT64, TSEncoding.RLE));

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

          for (int seriesID = 0; seriesID < series_num; seriesID++) {
            String series = seriesList.get(seriesID);

            if (seriesID == 0) {
              long num = Math.round(1e10 * random.nextGaussian());
              ((long[]) values[seriesID])[row] = num;
            } else if (seriesID == 1) {
              long num = 1; // = ((random.nextInt() & 1) == 1) ? 1 : -1;
              num = num * (long) (Math.pow(10, 1 + random.nextDouble() * 17.5)); // log-uniform
              ((long[]) values[seriesID])[row] = num;
            } else if (seriesID == 2) {
              //              long num = ((index & 1) == 1) ? 1 : -1;
              long num = (long) (Math.pow(10, 1 + (17.5 * index / TOTAL_SIZE)));
              ((long[]) values[seriesID])[row] = num;
            } else if (seriesID == 3) {
              long num = (long) (-1e15 + 2e15 * random.nextDouble());
              ((long[]) values[seriesID])[row] = num;
            } else if (seriesID == 4) {
              long num = (long) (-1e15 + 2e15 / TOTAL_SIZE * index);
              ((long[]) values[seriesID])[row] = num;
            } else if (seriesID == 5) {
              long num =
                  (long) (-1e15 + 2e15 * Math.sin(Math.PI * 2 / 1048576 * (index & 1048575)));
              ((long[]) values[seriesID])[row] = num;
            } else if (seriesID == 6) {
              double num =
                  Math.pow(-1, random.nextInt(2))
                      * Math.pow(10, (2 * Math.pow(random.nextDouble(), 2) - 1) * 200);
              // signed (log-uniform) ^2
              ((double[]) values[seriesID])[row] = num;
            } else if (seriesID == 7) {
              double num = ((random.nextInt() & 1) == 1) ? 1 : -1;
              num = num * (Math.pow(10, (2 * random.nextDouble() - 1) * 200)); // signed log-uniform
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

  private void NewTXTTest() throws IoTDBConnectionException, StatementExecutionException {
    final int txt_id = 5;
    final int TOT_N = baseSize * txt_id;
    List<String> aggrList = new ArrayList<>();
    //    aggrList.add("exact_median_kll_floats");
    //    aggrList.add("exact_median_kll_stat");
    aggrList.add("exact_median_kll_floats_single");
    aggrList.add("exact_median_kll_stat_single");
    //    aggrList.add("exact_median_aggressive");

    int TEST_CASE = 12;
    long START_TIME;
    SessionDataSet dataSet;

    String device = "root.real.d0";
    for (int ratio = 100; ratio <= 100; ratio *= 10)
      for (int LEN = (int) 1e5 * ratio; LEN < (int) 1e6 * ratio; LEN += (int) 2e5 * ratio) {
        System.out.println("\t\t\tLEN:" + LEN);
        long[] LL = new long[TEST_CASE];
        long[] RR = new long[TEST_CASE];
        for (int i = 0; i < TEST_CASE; i++) {
          LL[i] = new Random().nextInt(TOT_N - LEN);
          RR[i] = LL[i] + LEN;
          //        System.out.println("\t\t\t"+LL[i]+"  "+RR[i]);
        }
        double[] relativeDelta = new double[2];
        int[] relativeCnt = new int[2];
        for (int aggrID = 0; aggrID < aggrList.size(); aggrID++) {
          String aggr = aggrList.get(aggrID);
          //        System.out.println(" [aggr] " + aggr);
          String series = "s" + txt_id;
          long t1 = System.currentTimeMillis(), tmpT;
          long[] times = new long[TEST_CASE];
          for (int i = 0; i < TEST_CASE; i++) {
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
            String str = dataSet.next().getFields().toString();
            if (i == TEST_CASE - 1) System.out.print(str);
            tmpT = System.currentTimeMillis();
            times[i] = tmpT - t1;
            t1 = tmpT;
            if (aggrID + 2 >= aggrList.size()) { // single iteration approx
              //            System.out.println("\t\t" + str);
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

              double tmpDelta = Math.abs(LEN / 2.0 - result2) / LEN;
              //            System.out.println("\t\t\t"+result2+"\t\t"+tmpDelta);
              int iii = aggrList.size() - 1 - aggrID;
              relativeDelta[iii] += tmpDelta;
              relativeCnt[iii]++;
            }
          }
          Arrays.sort(times);
          int cnt_cases = 0;
          tmpT = 0;
          for (int i = TEST_CASE / 4; i < TEST_CASE * 3 / 4; i++) {
            cnt_cases++;
            tmpT += times[i];
          }
          System.out.print("\t" + tmpT / cnt_cases);
          System.out.println();
        }
        System.out.println("\n\t\tstat single: " + relativeDelta[0] / relativeCnt[0] + "\n");
        System.out.println("\n\t\tfloats single: " + relativeDelta[1] / relativeCnt[1] + "\n");
      }
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

  private static void insertDataFromTXT(int TXT_ID, int series_id, long EX_BASE_TIME)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    final int real_N = baseSize;
    String series = "s" + series_id;
    if (EX_BASE_TIME == 0)
      session.createTimeseries(
          "root.real.d0." + series, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);

    MeasurementSchema schema = new MeasurementSchema(series, TSDataType.DOUBLE, TSEncoding.PLAIN);
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(schema);

    Random random = new Random(233);
    String filename = "tmp_" + TXT_ID + ".txt";
    String folder = "E:\\real-world data";
    String filepath = folder + "\\" + filename;
    File file = new File(filepath);
    BufferedInputStream fis = null;
    fis = new BufferedInputStream(new FileInputStream(file));
    BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(fis, StandardCharsets.UTF_8), 5 * 1024 * 1024); // 用5M的缓冲读取文本文件
    reader.readLine(); // ignore first line

    String device = "root.real.d0";
    int TABLET_NUM = real_N / TABLET_SIZE;
    for (int i = 0; i < TABLET_NUM; i++) {
      long BASE_TIME = EX_BASE_TIME + (long) i * TABLET_SIZE;
      Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);

      long[] timestamps = tablet.timestamps;
      Object[] values = tablet.values;

      for (long time = 0; time < TABLET_SIZE; time++) {
        int row = tablet.rowSize++;
        timestamps[row] = BASE_TIME + time;
        ((double[]) values[0])[row] = Double.parseDouble(reader.readLine());
        //          System.out.println("\t\t\t"+timestamps[row]);
      }
      session.insertTablet(tablet);
    }
  }

  private static void insertDataFromTXT(int TOT)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    for (int TXT_ID = 0; TXT_ID < TOT; TXT_ID++) {
      insertDataFromTXT(TXT_ID, TOT, (long) TXT_ID * baseSize);
    }
  }

  @Test
  public void insertDATA() {
    try {
      //      prepareTimeSeriesData();
      insertDataFromTXT(5);
      //      insertDataFromTXT(3, 3, 0);
    } catch (IoTDBConnectionException | StatementExecutionException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
