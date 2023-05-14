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
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
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
  private static final List<String> deviceList = new ArrayList<>(), sgList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  //  private static final int baseSize = (int) (1e9 + 1e7); // 8192 * 6713;
  private static final int baseSize = (int) (262144 * 100);
  private static final int TABLET_SIZE = 262144, TABLET_NUM = baseSize / TABLET_SIZE;
  private static final int deviceNumL = 0, deviceNumR = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;

  @BeforeClass
  public static void setUp() throws Exception {
    for (int i = deviceNumL; i < deviceNumR; i++) {
      deviceList.add("root.Synthetic.d" + i);
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
    //    System.out.println("\t\t????" + deviceList + "||||" + seriesList);
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

    double Emax = 300;
    //    double num;
    LogNormalDistribution Rlog1 = new LogNormalDistribution(4, 0.0004);
    UniformRealDistribution R01 = new UniformRealDistribution(0, 1);
    DoubleArrayList ddddd = new DoubleArrayList();
    for (int i = 0; i < baseSize; i++) {
      double num;
      if (R01.sample() < 0.1) num = Math.pow(10, Emax * (Math.pow(R01.sample(), 2) * 2 - 1));
      else num = Rlog1.sample();
      ddddd.add(num);
    }

    START_TIME = System.currentTimeMillis();
    XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(233);

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
              //              double Emax = 300;
              //              double num;
              //              LogNormalDistribution Rlog1 = new LogNormalDistribution(4, 0.0004);
              //              UniformRealDistribution R01 = new UniformRealDistribution(0, 1);
              //              num = Rlog1.sample();
              //              if (R01.sample() < 0.1)
              //                num = Math.pow(10, Emax * (Math.pow(R01.sample(), 2) * 2 - 1));
              //              ((double[]) values[seriesID])[row] = num;
              //              ddddd
              ((double[]) values[seriesID])[row] = ddddd.getDouble((int) timestamps[row]);
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
      session.executeNonQueryStatement("flush");
    }
    System.out.println(
        "\t\t create designed data cost time:\t" + (System.currentTimeMillis() - START_TIME));
  }

  static final long real_data_series_base_time = 1L << 32;

  private static void insertDataFromTXT()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    final int TEST_CASE = 1;
    String[] fileList = new String[10], sgName = new String[10];
    String sketch_size = "";
    fileList[0] = "bitcoin-s.csv";
    sgName[0] = "root.bitcoin" + sketch_size;
    fileList[1] = "gas-s.csv";
    sgName[1] = "root.gas" + sketch_size;
    fileList[2] = "power-s.csv";
    sgName[2] = "root.power" + sketch_size;
    //    fileList[1] = "tmp_3_55.txt";
    //    fileList[2] = "tmp_0_55.txt";
    //    fileList[3] = "tmp_2_55.txt";
    //    fileList[4] = "tmp_1_60.txt";
    //    fileList[5] = "tmp_0_131.txt";
    //    fileList[6] = "tmp_1_131.txt";
    //    fileList[7] = "tmp_0_356.txt";
    for (int fileID : new int[] {0, 1, 2}) {
      System.out.print("\t\t" + fileList[fileID] + "\t" + sketch_size + "\t\t");
      System.out.print("\t\t\t");

      String filename = fileList[fileID];
      String folder = "E:\\MAD-data";
      String filepath = folder + "\\" + filename;
      DoubleArrayList vv = new DoubleArrayList();

      for (int T = 0; T < TEST_CASE; T++) {
        try {
          session.executeNonQueryStatement("delete timeseries " + sgName[fileID] + ".d0.s0");
          session.executeNonQueryStatement("delete storage group " + sgName[fileID]);
          //          System.out.println("???");
          Thread.sleep(1000);
          //          System.out.println("!!!");
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

        //        Random random = new Random(233);

        int chunk_num = 0;
        String device = sgName[fileID] + ".d0";
        long CNT_TIME = 0; // new Date().getTime();
        long INGEST_TIME = 0;
        while (true) {
          //          vv.clear();
          if (vv.isEmpty()) {
            //            System.out.println("\t\t\treaddddddddddddddddddd");
            for (String tmps = reader.readLine();
                tmps != null && vv.size() < baseSize /*TABLET_SIZE * 200*/;
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
            if (++chunk_num == TABLET_NUM) break;
          }
          INGEST_TIME += new Date().getTime();
          if (chunk_num == TABLET_NUM) break;
          if (vv.size() < TABLET_SIZE) break;
        }
        //        System.out.print("\tingest_time:\t" + INGEST_TIME);
        System.out.print("\t" + INGEST_TIME);
        System.out.flush();
      }
      System.out.println();
    }
    //    System.out.println();
  }

  private static void append(int chunkToAppend)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    final int TEST_CASE = 1;
    String[] fileList = new String[10], sgName = new String[10];
    String sketch_size = "4096T32";
    fileList[0] = "1_bitcoin.csv";
    sgName[0] = "root.bitcoin" + sketch_size;
    fileList[1] = "2_SpacecraftThruster.txt";
    sgName[1] = "root.thruster" + sketch_size;
    fileList[2] = "3_taxipredition8M.txt";
    sgName[2] = "root.taxi" + sketch_size;
    fileList[3] = "4_wh.csv";
    sgName[3] = "root.wh" + sketch_size;
    for (int fileID : new int[] {1}) {
      System.out.println("APPEND to\t\t" + fileList[fileID] + "\t" + sketch_size + "\t\t");
      System.out.print("\t\t\t");

      String series = "s0";
      MeasurementSchema schema = new MeasurementSchema(series, TSDataType.DOUBLE, TSEncoding.PLAIN);
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(schema);

      //        Random random = new Random(233);

      int chunk_num = 0;
      String device = sgName[fileID] + ".d0";
      long CNT_TIME = new Date().getTime();
      long INGEST_TIME = 0;
      Random random = new Random(233);

      INGEST_TIME -= new Date().getTime();
      for (int i = 0; i < chunkToAppend; i++) {
        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        for (int j = 0; j < TABLET_SIZE; j++) {
          int row = tablet.rowSize++;
          timestamps[row] = CNT_TIME++;
          ((double[]) values[0])[row] = random.nextGaussian();
        }
        session.insertTablet(tablet);
      }
    }
  }

  @Test
  public void insertDATA() {
    try {
      //      for (int i = 0; i < 10; i++) prepareTimeSeriesData();
      insertDataFromTXT();
      //      append(5);
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
