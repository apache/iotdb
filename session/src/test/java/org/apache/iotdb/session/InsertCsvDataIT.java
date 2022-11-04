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
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

public class InsertCsvDataIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int baseSize = 4096 * 20000;
  private static final int TABLET_SIZE = 4096;
  private static final int deviceNumL = 0, deviceNumR = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;

  @BeforeClass
  public static void setUp() throws Exception {
    for (int i = deviceNumL; i < deviceNumR; i++) {
      deviceList.add("root.Summary1.d" + i);
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

  private static void prepareTimeSeriesData()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    System.out.println("\t\t????" + deviceList + "||||" + seriesList);
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

    for (int deviceID = 0; deviceID < deviceNumR - deviceNumL; deviceID++) {
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
          //          if (i % 2 == 1)
          //            timestamps[row] = /*BASE_TIME + time*/ +baseSize + random.nextInt(baseSize);
          //          else
          timestamps[row] = BASE_TIME + time;
          long index = timestamps[row];

          for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
            String series = seriesList.get(seriesID);

            if (seriesID == 0) {
              double num = random.nextDouble();
              ((double[]) values[seriesID])[row] = num;
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
        //        session.executeNonQueryStatement("flush");
      }
    }
    System.out.println(
        "\t\t create designed data cost time:" + (System.currentTimeMillis() - START_TIME));
  }

  static final long real_data_series_base_time = 1L << 32;

  private static void insertDataFromTXT()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
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

    for (int i = 0; i < 8; i++) {
      String series = "s" + i;
      session.createTimeseries(
          "root.real.d0." + series, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
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

      String device = "root.real.d0";
      long CNT_TIME = i * real_data_series_base_time;
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
    System.out.println("\t\t[WRITE FINISH]:\t" + (new Date().getTime() - START_TIME));
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

  //  @Test
  //  public void run() throws IoTDBConnectionException, StatementExecutionException, IOException {
  //    //        prepareTimeSeriesData();
  //    insertDataFromTXT();
  //  }
}
