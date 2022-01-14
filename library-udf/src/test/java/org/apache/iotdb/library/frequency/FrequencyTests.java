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

package org.apache.iotdb.library.frequency;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.fail;

public class FrequencyTests {
  protected static final int ITERATION_TIMES = 16384;
  protected static final int DELTA_T = 100;
  protected static final int PERIOD_1 = 10;
  protected static final int PEROID_2 = 200;
  protected static final double pi = Math.PI;

  @BeforeClass
  public static void setUp() throws Exception {
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(5)
        .setUdfTransformerMemoryBudgetInMB(5)
        .setUdfReaderMemoryBudgetInMB(5);
    EnvFactory.getEnv().initBeforeClass();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.vehicle"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s1"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s2"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.vehicle.d1.s3"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
            new PartialPath("root.vehicle.d2.s1"),
            TSDataType.DOUBLE,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null);
    IoTDB.metaManager.createTimeseries(
            new PartialPath("root.vehicle.d2.s2"),
            TSDataType.DOUBLE,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null);
  }

  private static void generateData() {
    double x = -100d, y = 100d; // borders of random value
    long a = 0, b = 1000000000;
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        statement.execute(
            String.format(
                "insert into root.vehicle.d1(timestamp,s1,s2,s3) values(%d,%f,%f,%f)",
                    (long)(i+1)*DELTA_T, Math.sin(i%PERIOD_1*2*pi/(double)PERIOD_1),
                    Math.sin(i%PEROID_2*2*pi/(double)PEROID_2),
                    0.5*Math.sin(i%PERIOD_1*pi)+Math.sin(i%PEROID_2*pi)));
      }
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1) values(%d, 2)",DELTA_T));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1) values(%d, 7)",2*DELTA_T));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1) values(%d, 4)",3*DELTA_T));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s1) values(%d, 9)",4*DELTA_T));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s2) values(%d, 1)",DELTA_T));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s2) values(%d, 0)",2*DELTA_T));
      statement.execute(String.format("insert into root.vehicle.d2(timestamp,s2) values(%d, 1)",3*DELTA_T));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create function conv as 'org.apache.iotdb.library.dquality.UDTFConv'");
      statement.execute(
          "create function deconv as 'org.apache.iotdb.library.dquality.UDTFDeconv'");
      statement.execute(
          "create function dwt as 'org.apache.iotdb.library.dquality.UDTFDWT'");
      statement.execute(
          "create function fft as 'org.apache.iotdb.library.dquality.UDTFFFT'");
      statement.execute(
              "create function highpass as 'org.apache.iotdb.library.dquality.UDTFHighPass'");
      statement.execute(
              "create function idwt as 'org.apache.iotdb.library.dquality.UDTFIDWT'");
      statement.execute(
              "create function lowpass as 'org.apache.iotdb.library.dquality.UDTFLowPass'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig()
        .setUdfCollectorMemoryBudgetInMB(100)
        .setUdfTransformerMemoryBudgetInMB(100)
        .setUdfReaderMemoryBudgetInMB(100);
  }

  // No possible tests for IDWT, IFFT
  @Test
  public void testConv1() {
    String sqlStr = "select conv(d1.s1, d1.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result = Double.parseDouble(resultSet.getString(1));
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDeconv1() {
    String sqlStr = "select deconv(d2.s1,d2.s2) from root.vehicle";
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result1 = Double.parseDouble(resultSet.getString(1));
      Double result2 = Double.parseDouble(resultSet.getString(2));
      assert Math.abs(result1-2d)<1e-5 && Math.abs(result2-7d)<1e-5;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDeconv2() {
    String sqlStr = "select deconv(d2.s1,d2.s2,'result'='remainder') from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      Double result1 = Double.parseDouble(resultSet.getString(1));
      Double result2 = Double.parseDouble(resultSet.getString(2));
      Double result3 = Double.parseDouble(resultSet.getString(3));
      Double result4 = Double.parseDouble(resultSet.getString(4));
      assert Math.abs(result1)<1e-5 && Math.abs(result2)<1e-5 && Math.abs(result3-2d)<1e-5 && Math.abs(result4-2d)<1e-5;
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDWT1(){
    String sqlStr = "select dwt(d2.s3,'method'='haar') from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testDWT2(){
    String sqlStr = "select dwt(d2.s3,'method'='DB4') from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testFFT1(){
    String sqlStr = "select fft(d1.s1) from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testHighPass1(){
    String sqlStr = "select highpass(d1.s3,'wpass'='0.5') from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet1 = statement.executeQuery(sqlStr);
      ResultSet resultSet2 = statement.executeQuery("select d1.s1 from root.vehicle");
      for(int i=1;i<ITERATION_TIMES;++i){
        Double result1 = Double.parseDouble(resultSet1.getString(i));
        Double result2 = Double.parseDouble(resultSet2.getString(i));
        assert Math.abs(result1-0.5*result2)<1e-2;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLowPass1(){
    String sqlStr = "select lowpass(d1.s3,'wpass'='0.5') from root.vehicle";
    try (Connection connection =
                 DriverManager.getConnection(
                         Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
         Statement statement = connection.createStatement()) {
      ResultSet resultSet1 = statement.executeQuery(sqlStr);
      ResultSet resultSet2 = statement.executeQuery("select d1.s2 from root.vehicle");
      for(int i=1;i<ITERATION_TIMES;++i){
        Double result1 = Double.parseDouble(resultSet1.getString(i));
        Double result2 = Double.parseDouble(resultSet2.getString(i));
        assert Math.abs(result1-result2)<1e-2;
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
