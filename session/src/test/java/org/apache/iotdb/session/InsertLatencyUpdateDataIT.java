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

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.fail;

public class InsertLatencyUpdateDataIT {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final List<String> deviceList = new ArrayList<>();
  private static final List<Integer> sizeList = new ArrayList<>();
  private static final int TABLET_SIZE = 100000;
  private static final int baseSize = TABLET_SIZE * 200;
  private static final int device_num = 1;
  private static final long REVERSE_TIME = 1L << 60, UPDATE_ARRIVAL_TIME = 1L << 50;
  private static final List<String> seriesList = new ArrayList<>();
  private static final List<TSDataType> dataTypeList = new ArrayList<>();
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;
  static final double updateRate = 0.00;
  static final double mu = 2, sig = 3;
  static final int bits_per_key = 0;

  @BeforeClass
  public static void setUp() throws Exception {
    for (int i = 0; i < device_num; i++) {
      deviceList.add("root.update000_" + bits_per_key + "_" + (long) mu + (long) sig + ".d" + i);
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
      prepareTimeSeriesData(mu, sig, updateRate);
      //      insertDataFromTXT(5);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    session.close();
    if (inMemory) EnvironmentUtils.cleanEnv();
    CONFIG.setConcurrentCompactionThread(originCompactionThreadNum);
  }

  private static void prepareTimeSeriesData(double mu, double sig, double updateRate)
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

    //    List<LongLongPair> a = new ArrayList<>(baseSize);
    //    for (int i = 0; i < baseSize; i++) {
    //      a.add(
    //          PrimitiveTuples.pair(
    //              (long) i, (long) Math.round(i + Math.exp(mu + sig * random.nextGaussian()))));
    //      //      System.out.println("\t\t"+a.get(i).getOne()+"\t"+a.get(i).getTwo());
    //    }
    //    a.sort(Comparator.comparingLong(LongLongPair::getTwo));

    long[] aa;
    long[] bb;
    IntArrayList cc;
    aa = new long[baseSize];
    bb = new long[baseSize];
    cc = new IntArrayList(baseSize);
    int tablet_insert_num = (int) ((baseSize / TABLET_SIZE) * (1 - updateRate));
    int tablet_update_num = (baseSize / TABLET_SIZE) - tablet_insert_num;
    boolean tablet_updated[] = new boolean[tablet_insert_num];
    int tablet_to_update[] = new int[tablet_update_num];
    for (int i = 0; i < tablet_update_num; i++) {
      int TO_UPDATE = random.nextInt(tablet_insert_num);
      while (tablet_updated[TO_UPDATE]) TO_UPDATE = random.nextInt(tablet_insert_num);
      tablet_updated[TO_UPDATE] = true;
      tablet_to_update[i] = TO_UPDATE;
    }
    Arrays.sort(tablet_to_update);
    int[] aaDelta = new int[tablet_update_num];
    for (int i = 0; i < tablet_update_num; i++) {
      if (tablet_to_update[i] > 0 && !tablet_updated[tablet_to_update[i] - 1]) {
        int j = i + 1;
        while (j < tablet_update_num && tablet_to_update[j] - tablet_to_update[i] == j - i) j++;
        int tmpDelta = -random.nextInt(TABLET_SIZE / 2 + 1);
        for (int k = i; k < j; k++) aaDelta[k] += tmpDelta;
      }
      if (tablet_to_update[i] < tablet_insert_num - 1 && !tablet_updated[tablet_to_update[i] + 1]) {
        int j = i - 1;
        while (j >= 0 && tablet_to_update[j] - tablet_to_update[i] == j - i) j--;
        int tmpDelta = random.nextInt(TABLET_SIZE / 2 + 1);
        for (int k = j + 1; k <= i; k++) aaDelta[k] += tmpDelta;
      }
    }
    for (int i = 0; i < baseSize; i++) {
      cc.add(i);
      aa[i] = i;
      bb[i] = (long) Math.round(i + Math.exp(mu + sig * random.nextGaussian()));
      if (i / TABLET_SIZE >= tablet_insert_num && i % TABLET_SIZE == TABLET_SIZE - 1) {
        int ST_TO_UPDATE = tablet_to_update[i / TABLET_SIZE - tablet_insert_num];
        //        int aaDelta = 0;
        //        if(ST_TO_UPDATE>0&&!tablet_updated[ST_TO_UPDATE-1])
        //          aaDelta -=random.nextInt(TABLET_SIZE/2);
        //        if(ST_TO_UPDATE<tablet_update_num-1&&!tablet_updated[ST_TO_UPDATE+1])
        //          aaDelta +=random.nextInt(TABLET_SIZE/2);
        for (int j = 0; j < TABLET_SIZE; j++) {
          //          System.out.println("\t\t"+(ST_TO_UPDATE * TABLET_SIZE +
          // j+aaDelta[i/TABLET_SIZE-tablet_insert_num]));
          aa[i - TABLET_SIZE + 1 + j] =
              aa[ST_TO_UPDATE * TABLET_SIZE + j + aaDelta[i / TABLET_SIZE - tablet_insert_num]];
          bb[i - TABLET_SIZE + 1 + j] += UPDATE_ARRIVAL_TIME;
        }
      }
    }
    cc.sort((x, y) -> (Long.compare(bb[x], bb[y])));

    for (int deviceID = 0; deviceID < device_num; deviceID++) {
      String device = deviceList.get(deviceID);
      int TABLET_NUM = (baseSize / TABLET_SIZE) * (deviceID + 1);
      long TOTAL_SIZE = baseSize * (deviceID + 1);
      long index = 0;
      for (int i = -1; i < TABLET_NUM; i++) {

        Tablet tablet = new Tablet(device, schemaList, TABLET_SIZE);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        for (long time = 0; time < TABLET_SIZE; time++) {
          int row = tablet.rowSize++;

          if (i == -1) timestamps[row] = time + REVERSE_TIME;
          else timestamps[row] = aa[cc.getInt((int) index++)];
          //                    if (index < 200) System.out.println("\t" + timestamps[row]);

          for (int seriesID = START_SERIES; seriesID < series_num; seriesID++) {
            String series = seriesList.get(seriesID);

            if (seriesID == 0) {
              double num = random.nextGaussian();
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
      for (String update : new String[] {"000", "010", "020", "049"})
        for (double mu = 2.0, sig = 0.0; sig <= 3.3; sig += 1.0) {
          double updateRate = Integer.parseInt(update) / 100.0;
          deviceList.set(
              0,
              "root.update" + update + "_" + bits_per_key + "_" + (long) mu + (long) sig + ".d0");
          prepareTimeSeriesData(mu, sig, updateRate);
        }
      //      insertDataFromTXT();
      //      insertDataFromTXT();
      //      insertDataFromTXT(3, 3, 0);
    } catch (IoTDBConnectionException | StatementExecutionException | IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void executeStatement()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    SessionDataSet dataSet;
    dataSet = session.executeQueryStatement("show timeseries");
    while (dataSet.hasNext()) System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    long ST;

    ST = new Date().getTime();
    for (int i = 0; i < 1; i++)
      dataSet =
          session.executeQueryStatement(
              "select exact_median_kll_stat_single(s0) from "
                  + deviceList.get(0)
                  + " where time < "
                  + REVERSE_TIME);
    while (dataSet.hasNext()) System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    System.out.println("\t\ttime:" + (new Date().getTime() - ST));

    ST = new Date().getTime();
    for (int i = 0; i < 1; i++)
      dataSet =
          session.executeQueryStatement(
              "select count(s0) from " + deviceList.get(0) + " where time < " + REVERSE_TIME);
    while (dataSet.hasNext()) System.out.println("[DEBUG]" + dataSet.next().getFields().toString());
    System.out.println("\t\ttime:" + (new Date().getTime() - ST));
  }
}
