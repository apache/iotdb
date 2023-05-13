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
import java.util.List;

import static org.junit.Assert.fail;

public class ExactQuantile_InsertUpdateData {
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private static Session session;
  private static int originCompactionThreadNum;
  private static final int TABLET_SIZE = 262144;
  private static final int series_num = 1;
  private static final int Long_Series_Num = 0;
  private static final boolean inMemory = false;
  static String sketch_size = "0Byte";
  static double UpdateRate = 0.1;
  static String UpdateRateStr;

  @BeforeClass
  public static void setUp() throws Exception {
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
    UpdateRateStr = String.valueOf((int) Math.round(UpdateRate * 10000));
    while (UpdateRateStr.length() < 5) UpdateRateStr = "0" + UpdateRateStr;
    //    System.out.println("\t\t\t"+UpdateRateStr);

    long START_TIME = System.currentTimeMillis();

    String seriesName = "root.syn" + sketch_size + UpdateRateStr + ".d0.s0",
        sgName = "root.syn" + sketch_size + UpdateRateStr;

    try {
      session.executeNonQueryStatement("delete storage group " + sgName);
      session.executeNonQueryStatement("delete timeseries " + seriesName);
      Thread.sleep(400);
    } catch (Exception e) {
      // no-op
    }

    List<MeasurementSchema> schemaList = new ArrayList<>();
    session.createTimeseries(
        seriesName, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
    schemaList.add(new MeasurementSchema("s0", TSDataType.DOUBLE, TSEncoding.PLAIN));

    XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(233);
    double Emax = 300;
    double num;
    LogNormalDistribution Rlog1 = new LogNormalDistribution(4, 0.0004);
    UniformRealDistribution R01 = new UniformRealDistribution(0, 1);
    String device = sgName + ".d0";
    int flushedSeqDataSize = 0, SeqT = 0;

    Tablet seqTablet = new Tablet(device, schemaList, TABLET_SIZE);
    long[] seqT = seqTablet.timestamps;
    double[] seqV = (double[]) (seqTablet.values[0]);

    Tablet unSeqTablet = new Tablet(device, schemaList, TABLET_SIZE);
    long[] unSeqT = unSeqTablet.timestamps;
    double[] unSeqV = (double[]) (unSeqTablet.values[0]);

    int synN = (int) (1e8 + 1e8);
    for (int i = 0; i < synN; i++) {
      if (SeqT < synN * (1 - UpdateRate)
          && (flushedSeqDataSize == 0 || random.nextDoubleFast() >= UpdateRate)) {
        int row = seqTablet.rowSize++;
        seqT[row] = SeqT++;
        if (R01.sample() < 0.1) num = Math.pow(10, Emax * (Math.pow(R01.sample(), 2) * 2 - 1));
        else num = Rlog1.sample();
        seqV[row] = num;
        if (seqTablet.rowSize == TABLET_SIZE) {
          session.insertTablet(seqTablet);
          flushedSeqDataSize += TABLET_SIZE;
          seqTablet = new Tablet(device, schemaList, TABLET_SIZE);
          seqT = seqTablet.timestamps;
          seqV = (double[]) (seqTablet.values[0]);
          //          System.out.println("\t\tinsert seq TABLET");
        }
      } else {

        int row = unSeqTablet.rowSize++;
        unSeqT[row] = random.nextInt(flushedSeqDataSize);
        unSeqV[row] = random.nextGaussian();
        if (unSeqTablet.rowSize == TABLET_SIZE) {
          session.insertTablet(unSeqTablet);
          unSeqTablet = new Tablet(device, schemaList, TABLET_SIZE);
          unSeqT = unSeqTablet.timestamps;
          unSeqV = (double[]) (unSeqTablet.values[0]);
          //          System.out.println("\t\tinsert unseq TABLET");
        }
      }
    }

    session.executeNonQueryStatement("flush");
    if (seqTablet.rowSize > 0) {
      session.insertTablet(seqTablet);
      session.executeNonQueryStatement("flush");
    }

    if (unSeqTablet.rowSize > 0) {
      session.insertTablet(unSeqTablet);
      session.executeNonQueryStatement("flush");
    }

    System.out.println(
        "\t\t create synthetic data cost time:" + (System.currentTimeMillis() - START_TIME));
  }

  static final long real_data_series_base_time = 1L << 32;

  private static void insertUpdateDataFromTXT()
      throws IoTDBConnectionException, StatementExecutionException, IOException {
    UpdateRateStr = String.valueOf((int) Math.round(UpdateRate * 10000));
    while (UpdateRateStr.length() < 5) UpdateRateStr = "0" + UpdateRateStr;

    String[] fileList = new String[10], sgName = new String[10];
    fileList[0] = "1_bitcoin.csv";
    sgName[0] = "root.bitcoin" + sketch_size + UpdateRateStr;
    fileList[1] = "2_SpacecraftThruster.txt";
    sgName[1] = "root.thruster" + sketch_size + UpdateRateStr;
    fileList[2] = "3_taxipredition8M.txt";
    sgName[2] = "root.taxi" + sketch_size + UpdateRateStr;

    for (int fileID : new int[] {0, 1, 2}) {

      String filename = fileList[fileID];
      String folder = "D:\\Study\\Lab\\iotdb\\add_quantile_to_aggregation\\test_project_2";
      String filepath = folder + "\\" + filename;

      String series = "s0";
      String device = sgName[fileID] + ".d0";
      String seriesName = sgName[fileID] + ".d0.s0";
      System.out.println("\t\t" + seriesName);
      System.out.print("\t\t\t");
      try {
        session.executeNonQueryStatement("delete storage group " + sgName[fileID]);
        session.executeNonQueryStatement("delete timeseries " + seriesName);
        Thread.sleep(400);
      } catch (Exception e) {
        // no-op
      }
      File file = new File(filepath);
      BufferedInputStream fis = null;
      fis = new BufferedInputStream(new FileInputStream(file));
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8), 50 * 1024 * 1024);
      reader.readLine(); // ignore first line!
      int realN = 8192 * 6713;
      DoubleArrayList vv = new DoubleArrayList();
      for (int i = 0; i < realN; i++) vv.add(Double.parseDouble(reader.readLine()));

      session.createTimeseries(
          seriesName, TSDataType.DOUBLE, TSEncoding.PLAIN, CompressionType.SNAPPY);
      MeasurementSchema schema = new MeasurementSchema("s0", TSDataType.DOUBLE, TSEncoding.PLAIN);
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(schema);

      Tablet seqTablet = new Tablet(device, schemaList, TABLET_SIZE);
      long[] seqT = seqTablet.timestamps;
      double[] seqV = (double[]) (seqTablet.values[0]);

      Tablet unSeqTablet = new Tablet(device, schemaList, TABLET_SIZE);
      long[] unSeqT = unSeqTablet.timestamps;
      double[] unSeqV = (double[]) (unSeqTablet.values[0]);

      int flushedSeqDataSize = 0, SeqT = 0;
      XoRoShiRo128PlusRandom random = new XoRoShiRo128PlusRandom(233);
      for (int i = 0; i < realN; i++) {
        if (SeqT < realN * (1 - UpdateRate)
            && (flushedSeqDataSize == 0 || random.nextDoubleFast() >= UpdateRate)) {
          int row = seqTablet.rowSize++;
          seqT[row] = SeqT++;
          seqV[row] = vv.getDouble(i);
          if (seqTablet.rowSize == TABLET_SIZE) {
            session.insertTablet(seqTablet);
            flushedSeqDataSize += TABLET_SIZE;
            seqTablet = new Tablet(device, schemaList, TABLET_SIZE);
            seqT = seqTablet.timestamps;
            seqV = (double[]) (seqTablet.values[0]);
            //          System.out.println("\t\tinsert seq TABLET");
          }
        } else {

          int row = unSeqTablet.rowSize++;
          unSeqT[row] = random.nextInt(flushedSeqDataSize);
          unSeqV[row] = vv.getDouble(i);
          if (unSeqTablet.rowSize == TABLET_SIZE) {
            session.insertTablet(unSeqTablet);
            unSeqTablet = new Tablet(device, schemaList, TABLET_SIZE);
            unSeqT = unSeqTablet.timestamps;
            unSeqV = (double[]) (unSeqTablet.values[0]);
            //          System.out.println("\t\tinsert unseq TABLET");
          }
        }
      }

      session.executeNonQueryStatement("flush");
      if (seqTablet.rowSize > 0) {
        session.insertTablet(seqTablet);
        session.executeNonQueryStatement("flush");
      }

      if (unSeqTablet.rowSize > 0) {
        session.insertTablet(unSeqTablet);
        session.executeNonQueryStatement("flush");
      }
    }
  }

  @Test
  public void insertDATA() {
    try {
      for (double upd : new double[] {0, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5}) {
        UpdateRate = upd;
        prepareTimeSeriesData();
        insertUpdateDataFromTXT();
      }
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
