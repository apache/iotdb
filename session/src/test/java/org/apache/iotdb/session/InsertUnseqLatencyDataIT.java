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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

public class InsertUnseqLatencyDataIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int TABLET_SIZE = 8192;
  private static final int baseSize = TABLET_SIZE * 6713; // 6713;
  private static final int device_num = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;
  static final double mu = 3.0, sig = 2.6; // sig: 0,1.0,2.1,2.4,2.6
  static final String muS = Integer.toString((int) (Math.round(mu * 10)));
  static final String sigS = Integer.toString((int) (Math.round(sig * 10)));

  @BeforeClass
  public static void setUp() throws Exception {
    System.out.println("\t\tfreeMem\t" + (Runtime.getRuntime().freeMemory()) / (1024 * 1024.0));
    System.out.println("\t\tmaxMem\t" + (Runtime.getRuntime().maxMemory()) / (1024 * 1024.0));
    System.out.println("\t\ttotalMem\t" + (Runtime.getRuntime().totalMemory()) / (1024 * 1024.0));
    for (int i = 0; i < device_num; i++) {
      deviceList.add("root.sst_latency_" + muS + "_" + sigS + ".d" + i);
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
      prepareTimeSeriesData(mu, sig);
      //      insertDataFromTXT(5);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  private static void prepareTimeSeriesData(double mu, double sig)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    System.out.println("\t\t????" + deviceList + "||||" + seriesList);

    long START_TIME = System.currentTimeMillis();
    final int START_SERIES = 0;
    for (String device : deviceList) {
      for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
        String series = seriesList.get(seriesID);
        try {
          session.executeNonQueryStatement(
              "delete storage group " + device.substring(0, device.length() - 3));
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

    Random random = new Random(233);
    //    long[] aa;
    long[] bb;
    IntArrayList cc;
    //    aa = new long[baseSize];
    bb = new long[baseSize];
    System.out.println("\t\tfreeMem\t" + (Runtime.getRuntime().freeMemory()) / (1024 * 1024.0));
    System.out.println("\t\tmaxMem\t" + (Runtime.getRuntime().maxMemory()) / (1024 * 1024.0));
    System.out.println("\t\ttotalMem\t" + (Runtime.getRuntime().totalMemory()) / (1024 * 1024.0));
    cc = new IntArrayList(baseSize);
    for (int i = 0; i < baseSize; i++) {
      cc.add(i);
      //      aa[i] = i;
      bb[i] = (long) Math.round(i + Math.exp(mu + sig * random.nextGaussian()));
    }
    cc.sort((x, y) -> (Long.compare(bb[x], bb[y])));

    for (int deviceID = 0; deviceID < device_num; deviceID++) {
      String device = deviceList.get(deviceID);

      //      ArrayList<Double> tmpList = new ArrayList<>();
      //      for (int seriesID = 0; seriesID < series_num; seriesID++) tmpList.add(-233.0);
      //      session.insertRecord(device, 1L << 40, seriesList, dataTypeList, tmpList.toArray());
      //      session.executeNonQueryStatement("flush");

      Tablet unSeqTablet =
          new Tablet(device, schemaList, TABLET_SIZE); //  重要！ 先插一批时间戳极大的数据并且flush，这样后续数据才会全部划归乱序区。
      for (int i = 0; i < TABLET_SIZE; i++) {
        unSeqTablet.rowSize++;
        unSeqTablet.timestamps[i] = (1L << 40) + i;
        ((double[]) unSeqTablet.values[0])[i] = -2.33;
      }
      session.insertTablet(unSeqTablet);
      session.executeNonQueryStatement("flush");

      int TABLET_NUM = (baseSize / TABLET_SIZE) * (deviceID + 1);
      long TOTAL_SIZE = baseSize * (deviceID + 1);
      long index = 0;
      for (int i = 0; i < TABLET_NUM; i++) {
        //        long BASE_TIME;
        //        if (i == 0) BASE_TIME = REVERSE_TIME;
        //        else BASE_TIME = (long) (i - 1) * TABLET_SIZE + 1;

        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < TABLET_SIZE; index++, time++) {
          int row = tablet.rowSize++;
          timestamps[row] = cc.getInt((int) index); // aa[cc.getInt((int) index)];
          //          if (index < 100) System.out.println("\t" + timestamps[row]);
          //          if (i == 0) timestamps[row] += 1L << 30;

          for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
            String series = seriesList.get(seriesID);

            if (seriesID == 0) {
              double num = random.nextGaussian();
              ((double[]) values[seriesID])[row] = num;
            }
          }
        }
        session.insertTablet(tablet);
        //        session.executeNonQueryStatement("flush");
      }
      //      session.executeNonQueryStatement("flush");
    }
    System.out.println(
        "\t\t create designed data cost time:" + (System.currentTimeMillis() - START_TIME));
  }

  private static void insertDataFromFile(int fileID)
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    long START_TIME = System.currentTimeMillis();

    Random random = new Random(233);
    //    long[] aa;
    int[] bb;
    IntArrayList cc;
    //    aa = new long[baseSize];
    bb = new int[baseSize];
    cc = new IntArrayList(baseSize);
    for (int i = 0; i < baseSize; i++) {
      cc.add(i);
      //      aa[i] = i;
      bb[i] = (int) Math.round(i + Math.exp(mu + sig * random.nextGaussian()));
    }
    cc.sort((x, y) -> (Long.compare(bb[x], bb[y])));

    String[] fileList = new String[10], fileName = new String[10];
    String folder = "D:\\Study\\Lab\\iotdb\\add_quantile_to_aggregation\\test_project_2";
    fileList[1] = "1_bitcoin.csv";
    fileList[2] = "2_SpacecraftThruster.txt";
    fileList[3] = "3_taxipredition8M.txt";
    fileList[4] = "4_wh.csv";
    fileName[1] = "bitcoin";
    fileName[2] = "thruster";
    fileName[3] = "taxi";
    fileName[4] = "wh";

    {
      String series = "s0";
      String storage_group = "root.real_" + fileName[fileID] + "_latency_" + muS + "_" + sigS;
      String device = storage_group + ".d0";
      try {
        session.executeNonQueryStatement("delete storage group " + storage_group);
      } catch (Exception e) {
        // no-op
      }
      session.createTimeseries(
          device + "." + series, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);

      MeasurementSchema schema = new MeasurementSchema(series, TSDataType.DOUBLE, TSEncoding.PLAIN);
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(schema);

      Tablet unSeqTablet =
          new Tablet(device, schemaList, TABLET_SIZE); //  重要！ 先插一批时间戳极大的数据并且flush，这样后续数据才会全部划归乱序区。
      for (int i = 0; i < TABLET_SIZE; i++) {
        unSeqTablet.rowSize++;
        unSeqTablet.timestamps[i] = (1L << 40) + i;
        ((double[]) unSeqTablet.values[0])[i] = -2.33;
      }
      session.insertTablet(unSeqTablet);
      session.executeNonQueryStatement("flush");

      String filename = fileList[fileID];
      String filepath = folder + "\\" + filename;
      File file = new File(filepath);
      BufferedInputStream fis = null;
      fis = new BufferedInputStream(new FileInputStream(file));
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8), 50 * 1024 * 1024);
      reader.readLine(); // ignore first line!
      String tmps;
      boolean over_flag = false;
      long index = 0;
      double[] vv = new double[baseSize];
      while (!over_flag && index < baseSize) {
        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        for (int j = 0; j < TABLET_SIZE; index++, j++) {
          if ((tmps = reader.readLine()) != null) {
            vv[(int) index] = Double.parseDouble(tmps);
          } else {
            over_flag = true;
            break;
          }
        }
      }

      index = 0;
      for (int i = 0; i < baseSize / TABLET_SIZE; i++) {
        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);
        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;
        for (long time = 0; time < TABLET_SIZE; index++, time++) {
          int row = tablet.rowSize++;
          timestamps[row] = cc.getInt((int) index);
          //          double num = random.nextGaussian();
          ((double[]) values[0])[row] = vv[cc.getInt((int) index)];
        }
        session.insertTablet(tablet);
        //        session.executeNonQueryStatement("flush");
      }
    }
    System.out.println(
        "\t\t create designed data cost time:" + (System.currentTimeMillis() - START_TIME));
  }

  @Test
  public void insertDATA() {
    try {
      //            prepareTimeSeriesData(mu, sig);
      insertDataFromFile(1);
      //      insertDataFromTXT();
      //      insertDataFromTXT(3, 3, 0);
    } catch (IoTDBConnectionException | StatementExecutionException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  //  @Test
  //  public void executeStatement()
  //      throws IoTDBConnectionException, StatementExecutionException, IOException {
  //    SessionDataSet dataSet;
  //    dataSet = session.executeQueryStatement("show timeseries");
  //    while (dataSet.hasNext()) System.out.println("[DEBUG]" +
  // dataSet.next().getFields().toString());
  //    long ST;
  //
  //    ST = new Date().getTime();
  //    for (int i = 0; i < 1; i++)
  //      dataSet = session.executeQueryStatement("select count(s0) from " + deviceList.get(0));
  //    System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
  //    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
  //  }
}
