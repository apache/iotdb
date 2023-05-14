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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

public class InsertOverlapDataIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int baseSize = 10000 * 200; // 7989 * (12518 - 1);
  private static final int TABLET_SIZE = 10000;
  private static final int device_num = 1;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;

  @BeforeClass
  public static void setUp() throws Exception {
    for (int i = 0; i < device_num; i++) {
      deviceList.add("root.disorder_0.d" + i);
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
      prepareTimeSeriesData(0.2);
      //      insertDataFromTXT(5);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  private static void prepareTimeSeriesData(double overlap_rate)
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
    long REVERSE_TIME = 1L << 40;
    long delta_time = (long) (TABLET_SIZE * overlap_rate * 2);
    if (delta_time % 2 == 0) delta_time++;

    for (int deviceID = 0; deviceID < device_num; deviceID++) {
      String device = deviceList.get(deviceID);
      int TABLET_NUM = (baseSize / TABLET_SIZE) * (deviceID + 1);
      long TOTAL_SIZE = baseSize * (deviceID + 1);
      long index = 0;
      for (int i = 0; i < TABLET_NUM; i++) {
        long BASE_TIME;
        if (i == 0) BASE_TIME = REVERSE_TIME;
        else BASE_TIME = (long) (i) * TABLET_SIZE + 1;

        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < TABLET_SIZE; index++, time++) {
          int row = tablet.rowSize++;
          timestamps[row] = (BASE_TIME + time) * 2;
          if (index / TABLET_SIZE % 20 == 0 && index % TABLET_SIZE <= 2)
            timestamps[row] -= TABLET_SIZE * 2 + delta_time;
          else if (index / TABLET_SIZE % 30 == 3 && index % TABLET_SIZE <= 2)
            timestamps[row] -= TABLET_SIZE * 4 + delta_time;
          else if (index / TABLET_SIZE % 30 == 4 && index % TABLET_SIZE <= 2)
            timestamps[row] -= TABLET_SIZE * 6 + delta_time;
          else if (index / TABLET_SIZE % 30 == 5 && index % TABLET_SIZE <= 2)
            timestamps[row] -= TABLET_SIZE * 8 + delta_time;
          else if (i > 0
              && /*index / TABLET_SIZE % 2 == 0 && */ index % TABLET_SIZE
                  < TABLET_SIZE * overlap_rate) timestamps[row] -= delta_time;

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

  static final long real_data_series_base_time = 1L << 32;

  private static void insertDataFromTXT()
      throws IoTDBConnectionException, StatementExecutionException, IOException {}

  @Test
  public void insertDATA() {
    try {

      prepareTimeSeriesData(0.02);
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
    final int TEST_CASE = 8;
    int T = 0;
    for (double overlap = 0; overlap < 0.25; overlap += 0.008 * Math.pow(1.08, T)) {
      T++;
      System.out.print("\t\t\t\t" + overlap);
      prepareTimeSeriesData(overlap);
      session.executeNonQueryStatement("delete timeseries root.extreme.**");
      session.executeNonQueryStatement("delete storage group root.extreme");
      SessionDataSet dataSet;
      dataSet =
          session.executeQueryStatement(
              "select count(s0) from root.extreme.d0 where time<2147483647");
      long N = Long.parseLong(dataSet.next().getFields().toString());
      long time = 0, timeOverlap = 0, START_TIME;

      dataSet =
          session.executeQueryStatement(
              "select exact_median_kll_stat_overlap_single(s0) "
                  + "from root.extreme.d0 where time<2147483647");
      double vOverlap = Double.parseDouble(dataSet.next().getFields().toString());

      START_TIME = new Date().getTime();
      for (int i = 0; i < TEST_CASE; i++) {
        session.executeQueryStatement(
            "select exact_median_kll_stat_overlap_single(s0) "
                + "from root.extreme.d0 where time<2147483647");
      }
      timeOverlap += new Date().getTime() - START_TIME;

      // --------------

      dataSet =
          session.executeQueryStatement(
              "select exact_median_kll_stat_single(s0) "
                  + "from root.extreme.d0 where time<2147483647");
      double v = Double.parseDouble(dataSet.next().getFields().toString());
      START_TIME = new Date().getTime();
      for (int i = 0; i < TEST_CASE; i++) {
        session.executeQueryStatement(
            "select exact_median_kll_stat_single(s0) "
                + "from root.extreme.d0 where time<2147483647");
      }
      time += new Date().getTime() - START_TIME;

      //      System.out.print("\t\t\t\t"+overlap+"\t"+);
    }
  }
}
