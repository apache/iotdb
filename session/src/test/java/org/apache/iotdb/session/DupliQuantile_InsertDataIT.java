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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.util.XoRoShiRo128PlusPlusRandomGenerator;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.fail;

public class DupliQuantile_InsertDataIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>(), sgList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  // private static final int baseSize = (int) (1e9 + 1e8);
  // private static final int baseSize = (int) (262144 * 100);
  private static final int[] baseSize = new int[300];
  private static final int TABLET_SIZE = 262144;
  private static final int deviceNumL = 0, deviceNumR = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;

  @BeforeClass
  public static void setUp() throws Exception {
    baseSize[1] = baseSize[2] = baseSize[3] = baseSize[4] = (int) 3.1e7;
    for (int i = 10 + 1; i <= 10 + 14; i++) baseSize[i] = (int) 3.1e7;
    for (int i = 100 + 1; i <= 100 + 10; i++) baseSize[i] = (int) 3.1e7;
    for (int i = deviceNumL; i < deviceNumR; i++) {
      deviceList.add("root.Synthetic512ByteDD.d" + i);
      sizeList.add(baseSize[0] * (i + 1));
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
      // insertDataFromTXT();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  private static void prepareTimeSeriesData()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    // System.out.println("\t\t????" + deviceList + "||||" + seriesList);
    long START_TIME = System.currentTimeMillis();
    final int START_SERIES = 0;
    for (String device : deviceList) {
      String sgName = device.substring(0, device.lastIndexOf(".d"));
      for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
        String series = seriesList.get(seriesID);
        try {
          session.executeNonQueryStatement("delete storage group " + sgName);
          Thread.sleep(500);
        } catch (Exception e) {
          // no-op
        }
        session.createTimeseries(
            device + "." + series,
            dataTypeList.get(seriesID),
            // TSEncoding.PLAIN,
            // CompressionType.SNAPPY);
            TSEncoding.GORILLA,
            CompressionType.UNCOMPRESSED);
      }
    }

    List<MeasurementSchema> schemaList = new ArrayList<>();

    for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
      schemaList.add(
          new MeasurementSchema(
              seriesList.get(seriesID), dataTypeList.get(seriesID), TSEncoding.PLAIN));
    }

    LogNormalDistribution Rlog1 =
        new LogNormalDistribution(new XoRoShiRo128PlusPlusRandomGenerator(233), 1.0, 2.0);
    DoubleArrayList ddddd = new DoubleArrayList();
    for (int i = 0; i < baseSize[0]; i++) {
      ddddd.add(Rlog1.sample());
    }

    START_TIME = System.currentTimeMillis();
    XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(233);

    for (int deviceID = 0; deviceID < deviceNumR - deviceNumL; deviceID++) {
      String device = deviceList.get(deviceID);
      int TABLET_NUM = (baseSize[0] / TABLET_SIZE) * (deviceID + 1);
      long TOTAL_SIZE = baseSize[0] * (deviceID + 1);
      for (int i = 0; i < TABLET_NUM; i++) {
        long BASE_TIME = (long) i * TABLET_SIZE;
        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < TABLET_SIZE; time++) {
          int row = tablet.rowSize++;
          // if (i % 2 == 1)
          // timestamps[row] = /*BASE_TIME + time*/ +baseSize + random.nextInt(baseSize);
          // else
          timestamps[row] = BASE_TIME + time;
          long index = timestamps[row];

          for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
            String series = seriesList.get(seriesID);

            if (seriesID == 0) {
              // double Emax = 300;
              // double num;
              // LogNormalDistribution Rlog1 = new LogNormalDistribution(4, 0.0004);
              // UniformRealDistribution R01 = new UniformRealDistribution(0, 1);
              // num = Rlog1.sample();
              // if (R01.sample() < 0.1)
              // num = Math.pow(10, Emax * (Math.pow(R01.sample(), 2) * 2 - 1));
              // ((double[]) values[seriesID])[row] = num;
              // ddddd
              ((double[]) values[seriesID])[row] = ddddd.getDouble((int) timestamps[row]);
              // ((double[]) values[seriesID])[row] = tmpNorm.sample();
            } else if (seriesID == 1) {
              double num = index;
              ((double[]) values[seriesID])[row] = num;
            } else if (seriesID == 2) {
              long num = 1; // = ((random.nextInt() & 1) == 1) ? 1 : -1;
              num = num * (long) (Math.pow(10, 1 + random.nextDouble() * 17.5)); // iid log-uniform
              ((double[]) values[seriesID])[row] = Double.longBitsToDouble(num);
            } else if (seriesID == 3) {
              double num = (index % 7989) * Math.sin(index % 7989);
              ((double[]) values[seriesID])[row] = num;
            }
          }
        }
        session.insertTablet(tablet);
        // session.executeNonQueryStatement("flush");
      }
      session.executeNonQueryStatement("flush");
    }
    System.out.println(
        "\t\t create designed data cost time:\t" + (System.currentTimeMillis() - START_TIME));
  }

  static final long real_data_series_base_time = 1L << 32;

  private static void insertDataFromTXT()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    final int TEST_CASE = 1;
    String[] fileList = new String[300], sgName = new String[300];
    String sketch_size = "";
    fileList[1] = "DupliTorqueVoltage.txt";
    sgName[1] = "root.voltage" + sketch_size;
    fileList[2] = "DupliECommercePrice.txt";
    sgName[2] = "root.price" + sketch_size;
    fileList[3] = "DupliCustom.txt";
    sgName[3] = "root.custom" + sketch_size;
    fileList[4] = "Zipf3E7Alpha10.txt";
    sgName[4] = "root.zipf" + sketch_size;
    for (int i = 10 + 1; i <= 10 + 14; i++) {
      fileList[i] = "Zipf3E7Alpha" + (int) (i - 10) + ".txt";
      sgName[i] = "root.zipf" + (i - 10);
    }
    String muS = "5";
    for (int i = 100 + 1; i <= 100 + 10; i++) {
      fileList[i] = "Lognormal3E7Mu" + muS + "Sigma" + (int) (i - 100) * 2 + ".txt";
      sgName[i] = "root.lognormal" + (int) (i - 100) * 2;
    }
    for (int fileID = 1; fileID <= 110; fileID++ /* : new int[] {1, 2} */) {
      String filename = fileList[fileID];
      if (filename == null || filename.isEmpty()) {
        continue;
      }

      System.out.print("\t\t" + fileList[fileID] + "\t" + sketch_size + "\t\t");
      System.out.print("\t\t\t");

      String folder = "../../test_project";
      String filepath = folder + "/" + filename;
      DoubleArrayList vv = new DoubleArrayList();

      for (int T = 0; T < TEST_CASE; T++) {
        try {
          session.executeNonQueryStatement("delete timeseries " + sgName[fileID] + ".d0.s0");
          session.executeNonQueryStatement("delete storage group " + sgName[fileID]);
          // System.out.println("???");
          Thread.sleep(1000);
          // System.out.println("!!!");
        } catch (Exception e) {
          // no-op
        }
        File file = new File(filepath);
        BufferedInputStream fis = null;
        fis = new BufferedInputStream(new FileInputStream(file));
        BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(fis, StandardCharsets.UTF_8), 50 * 1024 * 1024);
        reader.readLine(); // ignore first line!

        long START_TIME = new Date().getTime();

        String series = "s0";
        session.createTimeseries(
            sgName[fileID] + ".d0.s0", TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
        MeasurementSchema schema =
            new MeasurementSchema(series, TSDataType.DOUBLE, TSEncoding.PLAIN);
        List<MeasurementSchema> schemaList = new ArrayList<>();
        schemaList.add(schema);

        // Random random = new Random(233);

        int chunk_num = 0;
        String device = sgName[fileID] + ".d0";
        long CNT_TIME = 0; // new Date().getTime();
        long INGEST_TIME = 0;
        while (true) {
          vv.clear();
          if (vv.isEmpty()) {
            // System.out.println("\t\t\treaddddddddddddddddddd");
            for (String tmps = reader.readLine();
                tmps != null && vv.size() < baseSize[fileID] /* TABLET_SIZE * 200 */;
                tmps = reader.readLine()) vv.add(Double.parseDouble(tmps));
          }
          INGEST_TIME -= new Date().getTime();
          for (int i = 0; i < vv.size() / TABLET_SIZE; i++) {
            Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);
            long[] timestamps = tablet.timestamps;
            Object[] values = tablet.values;
            for (int j = 0; j < TABLET_SIZE; j++) {
              int row = tablet.rowSize++;
              timestamps[row] = CNT_TIME++;
              ((double[]) values[0])[row] = vv.getDouble(i * TABLET_SIZE + j);
            }
            session.insertTablet(tablet);
            if (++chunk_num == baseSize[fileID] / TABLET_SIZE) break;
          }
          INGEST_TIME += new Date().getTime();
          if (chunk_num == baseSize[fileID] / TABLET_SIZE) break;
          if (vv.size() < TABLET_SIZE) break;
        }
        session.executeNonQueryStatement("flush");
        // System.out.print("\tingest_time:\t" + INGEST_TIME);
        System.out.print("\t" + INGEST_TIME + "ms");
        System.out.flush();
      }
      System.out.println();
    }
    // System.out.println();
  }

  @Test
  public void insertDATA() {
    try {
      // for (int i = 0; i < 10; i++) prepareTimeSeriesData();
      // prepareTimeSeriesData();
      insertDataFromTXT();
      // append(5);
      // insertDataFromTXT();
      // insertDataFromTXT(3, 3, 0);
    } catch (IoTDBConnectionException | StatementExecutionException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void executeStatement2()
      throws IoTDBConnectionException, StatementExecutionException, IOException,
          InterruptedException {
    SessionDataSet dataSet;
    double T = 1;
    long ST = new Date().getTime();
    for (int i = 0; i < T; i++) {
      // Thread.sleep(1000);
      dataSet =
          session.executeQueryStatement(
              "select dupli_quantile_kll_pair"
                  + "(s0,'memory'='128KB','quantile'='0.5') from "
                  + "root.voltage.d0"
                  + " where time>=0 and time<200000");
      String tmp = dataSet.next().getFields().toString();
      double returnValue = Double.parseDouble(tmp.substring(1, tmp.lastIndexOf(']')));
      System.out.println("\treturn_value:\t" + returnValue);
    }
    System.out.println("\t\tavgT:\t" + (new Date().getTime() - ST) / T);
  }
}
