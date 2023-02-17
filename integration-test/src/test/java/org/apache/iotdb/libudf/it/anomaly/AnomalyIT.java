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

package org.apache.iotdb.libudf.it.anomaly;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class AnomalyIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setUdfMemoryBudgetInMB(5);
    EnvFactory.getEnv().initClusterEnvironment();
    createTimeSeries();
    generateData();
    registerUDF();
  }

  private static void createTimeSeries() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch("create database root.vehicle");
      statement.addBatch(
          "create timeseries root.vehicle.d0.s0 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d1.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d2.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s5 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s6 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s7 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d3.s8 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d4.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d6.s1 with "
              + "datatype=int32, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d6.s2 with "
              + "datatype=int64, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d6.s3 with "
              + "datatype=float, "
              + "encoding=plain, "
              + "compression=uncompressed");
      statement.addBatch(
          "create timeseries root.vehicle.d6.s4 with "
              + "datatype=double, "
              + "encoding=plain, "
              + "compression=uncompressed");

      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void generateData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 100, 56.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 200, 55.1));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 300, 54.2));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 400, 56.3));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 500, 60.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 600, 60.5));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 700, 64.5));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 800, 69.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 900, 64.2));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1000, 62.3));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1100, 58.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1200, 58.9));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1300, 52.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1400, 62.3));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1500, 61.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1600, 64.2));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1700, 61.8));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1800, 64.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 1900, 63.0));
      statement.addBatch(
          String.format("insert into root.vehicle.d0(timestamp,s0) values(%d,%f)", 2000, 59.0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, -2, -2, -2, -2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 10, 10, 10, 10));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d1(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 50, 50, 50, 50));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, 100, 100, 100, 100));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, 150, 150, 150, 150));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 200, 200, 200, 200));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1100, 150, 150, 150, 150));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1200, 100, 100, 100, 100));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1300, 50, 50, 50, 50));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d2(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1400, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              200, 0, 0, 0, 0, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              300, 1, 1, 1, 1, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              400, 1, 1, 1, 1, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              500, 0, 0, 0, 0, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              600, -1, -1, -1, -1, -1, -1, -1, -1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              700, -1, -1, -1, -1, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d3(timestamp,s1,s2,s3,s4,s5,s6,s7,s8) values(%d,%d,%d,%d,%d,%d,%d,%d,%d)",
              800, 2, 2, 2, 2, 2, 2, 2, 2));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1100, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1200, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1300, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1400, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1500, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1600, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1700, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1800, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1900, 0, 0, 0, 0));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d4(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2000, 1, 1, 1, 1));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              100, 2002, 2002, 2002, 2002));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              200, 1946, 1946, 1946, 1946));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              300, 1958, 1958, 1958, 1958));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              400, 2012, 2012, 2012, 2012));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              500, 2051, 2051, 2051, 2051));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              600, 1898, 1898, 1898, 1898));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              700, 2014, 2014, 2014, 2014));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              800, 2052, 2052, 2052, 2052));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              900, 1935, 1935, 1935, 1935));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1000, 1901, 1901, 1901, 1901));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1100, 1972, 1972, 1972, 1972));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1200, 1969, 1969, 1969, 1969));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1300, 1984, 1984, 1984, 1984));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1400, 2018, 2018, 2018, 2018));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1500, 1484, 1484, 1484, 1484));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1600, 1055, 1055, 1055, 1055));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1700, 1050, 1050, 1050, 1050));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1800, 1023, 1023, 1023, 1023));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              1900, 1056, 1056, 1056, 1056));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2000, 978, 978, 978, 978));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2100, 1050, 1050, 1050, 1050));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2200, 1123, 1123, 1123, 1123));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2300, 1150, 1150, 1150, 1150));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2400, 1034, 1034, 1034, 1034));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2500, 950, 950, 950, 950));
      statement.addBatch(
          String.format(
              "insert into root.vehicle.d6(timestamp,s1,s2,s3,s4) values(%d,%d,%d,%d,%d)",
              2600, 1059, 1059, 1059, 1059));
      statement.executeBatch();
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create function iqr as 'org.apache.iotdb.library.anomaly.UDTFIQR'");
      statement.execute("create function ksigma as 'org.apache.iotdb.library.anomaly.UDTFKSigma'");
      statement.execute(
          "create function missdetect as 'org.apache.iotdb.library.anomaly.UDTFMissDetect'");
      statement.execute("create function lof as 'org.apache.iotdb.library.anomaly.UDTFLOF'");
      statement.execute("create function range as 'org.apache.iotdb.library.anomaly.UDTFRange'");
      statement.execute(
          "create function TwoSidedFilter as 'org.apache.iotdb.library.anomaly.UDTFTwoSidedFilter'");
      statement.execute(
          "create function outlier as 'org.apache.iotdb.library.anomaly.UDTFOutlier'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testIRQR1() {
    String sqlStr = "select iqr(d1.s1) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIQR2() {
    String sqlStr = "select iqr(d1.s2) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIQR3() {
    String sqlStr = "select iqr(d1.s3) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testIQR4() {
    String sqlStr = "select iqr(d1.s4) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma1() {
    String sqlStr = "select ksigma(d2.s1,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getInt(2);
      resultSet.next();
      double result2 = resultSet.getInt(2);
      resultSet.next();
      double result3 = resultSet.getInt(2);
      resultSet.next();
      double result4 = resultSet.getInt(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(50.0, result2, 0.01);
      Assert.assertEquals(50.0, result3, 0.01);
      Assert.assertEquals(0.0, result4, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma2() {
    String sqlStr = "select ksigma(d2.s2,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getLong(2);
      resultSet.next();
      double result2 = resultSet.getLong(2);
      resultSet.next();
      double result3 = resultSet.getLong(2);
      resultSet.next();
      double result4 = resultSet.getLong(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(50.0, result2, 0.01);
      Assert.assertEquals(50.0, result3, 0.01);
      Assert.assertEquals(0.0, result4, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma3() {
    String sqlStr = "select ksigma(d2.s3,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getFloat(2);
      resultSet.next();
      double result2 = resultSet.getFloat(2);
      resultSet.next();
      double result3 = resultSet.getFloat(2);
      resultSet.next();
      double result4 = resultSet.getFloat(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(50.0, result2, 0.01);
      Assert.assertEquals(50.0, result3, 0.01);
      Assert.assertEquals(0.0, result4, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testKSigma4() {
    String sqlStr = "select ksigma(d2.s4,\"k\"=\"1.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      Assert.assertEquals(0.0, result1, 0.01);
      Assert.assertEquals(50.0, result2, 0.01);
      Assert.assertEquals(50.0, result3, 0.01);
      Assert.assertEquals(0.0, result4, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect1() {
    String sqlStr = "select missdetect(d4.s1,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      boolean result1 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result2 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result3 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result4 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result5 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result6 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result7 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result8 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result9 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result10 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result11 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result12 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result13 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result14 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result15 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result16 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result17 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result18 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result19 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result20 = resultSet.getBoolean(2);
      Assert.assertFalse(result1);
      Assert.assertFalse(result2);
      Assert.assertFalse(result3);
      Assert.assertFalse(result4);
      Assert.assertTrue(result5);
      Assert.assertTrue(result6);
      Assert.assertTrue(result7);
      Assert.assertTrue(result8);
      Assert.assertTrue(result9);
      Assert.assertTrue(result10);
      Assert.assertTrue(result11);
      Assert.assertTrue(result12);
      Assert.assertTrue(result13);
      Assert.assertTrue(result14);
      Assert.assertTrue(result15);
      Assert.assertFalse(result16);
      Assert.assertFalse(result17);
      Assert.assertFalse(result18);
      Assert.assertFalse(result19);
      Assert.assertFalse(result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect2() {
    String sqlStr = "select missdetect(d4.s2,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      boolean result1 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result2 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result3 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result4 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result5 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result6 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result7 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result8 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result9 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result10 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result11 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result12 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result13 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result14 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result15 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result16 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result17 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result18 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result19 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result20 = resultSet.getBoolean(2);
      Assert.assertFalse(result1);
      Assert.assertFalse(result2);
      Assert.assertFalse(result3);
      Assert.assertFalse(result4);
      Assert.assertTrue(result5);
      Assert.assertTrue(result6);
      Assert.assertTrue(result7);
      Assert.assertTrue(result8);
      Assert.assertTrue(result9);
      Assert.assertTrue(result10);
      Assert.assertTrue(result11);
      Assert.assertTrue(result12);
      Assert.assertTrue(result13);
      Assert.assertTrue(result14);
      Assert.assertTrue(result15);
      Assert.assertFalse(result16);
      Assert.assertFalse(result17);
      Assert.assertFalse(result18);
      Assert.assertFalse(result19);
      Assert.assertFalse(result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect3() {
    String sqlStr = "select missdetect(d4.s3,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      boolean result1 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result2 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result3 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result4 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result5 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result6 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result7 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result8 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result9 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result10 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result11 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result12 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result13 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result14 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result15 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result16 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result17 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result18 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result19 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result20 = resultSet.getBoolean(2);
      Assert.assertFalse(result1);
      Assert.assertFalse(result2);
      Assert.assertFalse(result3);
      Assert.assertFalse(result4);
      Assert.assertTrue(result5);
      Assert.assertTrue(result6);
      Assert.assertTrue(result7);
      Assert.assertTrue(result8);
      Assert.assertTrue(result9);
      Assert.assertTrue(result10);
      Assert.assertTrue(result11);
      Assert.assertTrue(result12);
      Assert.assertTrue(result13);
      Assert.assertTrue(result14);
      Assert.assertTrue(result15);
      Assert.assertFalse(result16);
      Assert.assertFalse(result17);
      Assert.assertFalse(result18);
      Assert.assertFalse(result19);
      Assert.assertFalse(result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testMissDetect4() {
    String sqlStr = "select missdetect(d4.s4,'minlen'='10') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      boolean result1 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result2 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result3 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result4 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result5 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result6 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result7 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result8 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result9 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result10 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result11 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result12 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result13 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result14 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result15 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result16 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result17 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result18 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result19 = resultSet.getBoolean(2);
      resultSet.next();
      boolean result20 = resultSet.getBoolean(2);
      Assert.assertFalse(result1);
      Assert.assertFalse(result2);
      Assert.assertFalse(result3);
      Assert.assertFalse(result4);
      Assert.assertTrue(result5);
      Assert.assertTrue(result6);
      Assert.assertTrue(result7);
      Assert.assertTrue(result8);
      Assert.assertTrue(result9);
      Assert.assertTrue(result10);
      Assert.assertTrue(result11);
      Assert.assertTrue(result12);
      Assert.assertTrue(result13);
      Assert.assertTrue(result14);
      Assert.assertTrue(result15);
      Assert.assertFalse(result16);
      Assert.assertFalse(result17);
      Assert.assertFalse(result18);
      Assert.assertFalse(result19);
      Assert.assertFalse(result20);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF1() {
    String sqlStr = "select lof(d3.s1,d3.s5) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      Assert.assertEquals(3.8274824267668244, result1, 0.01);
      Assert.assertEquals(3.0117631741126156, result2, 0.01);
      Assert.assertEquals(2.838155437762879, result3, 0.01);
      Assert.assertEquals(3.0117631741126156, result4, 0.01);
      Assert.assertEquals(2.73518261244453, result5, 0.01);
      Assert.assertEquals(2.371440975708148, result6, 0.01);
      Assert.assertEquals(2.73518261244453, result7, 0.01);
      Assert.assertEquals(1.7561416374270742, result8, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF2() {
    String sqlStr = "select lof(d3.s2,d3.s6) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      Assert.assertEquals(3.8274824267668244, result1, 0.01);
      Assert.assertEquals(3.0117631741126156, result2, 0.01);
      Assert.assertEquals(2.838155437762879, result3, 0.01);
      Assert.assertEquals(3.0117631741126156, result4, 0.01);
      Assert.assertEquals(2.73518261244453, result5, 0.01);
      Assert.assertEquals(2.371440975708148, result6, 0.01);
      Assert.assertEquals(2.73518261244453, result7, 0.01);
      Assert.assertEquals(1.7561416374270742, result8, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF3() {
    String sqlStr = "select lof(d3.s3,d3.s7) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      Assert.assertEquals(3.8274824267668244, result1, 0.01);
      Assert.assertEquals(3.0117631741126156, result2, 0.01);
      Assert.assertEquals(2.838155437762879, result3, 0.01);
      Assert.assertEquals(3.0117631741126156, result4, 0.01);
      Assert.assertEquals(2.73518261244453, result5, 0.01);
      Assert.assertEquals(2.371440975708148, result6, 0.01);
      Assert.assertEquals(2.73518261244453, result7, 0.01);
      Assert.assertEquals(1.7561416374270742, result8, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testLOF4() {
    String sqlStr = "select lof(d3.s4,d3.s8) from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      Assert.assertEquals(3.8274824267668244, result1, 0.01);
      Assert.assertEquals(3.0117631741126156, result2, 0.01);
      Assert.assertEquals(2.838155437762879, result3, 0.01);
      Assert.assertEquals(3.0117631741126156, result4, 0.01);
      Assert.assertEquals(2.73518261244453, result5, 0.01);
      Assert.assertEquals(2.371440975708148, result6, 0.01);
      Assert.assertEquals(2.73518261244453, result7, 0.01);
      Assert.assertEquals(1.7561416374270742, result8, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange1() {
    String sqlStr =
        "select range(d1.s1,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getInt(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange2() {
    String sqlStr =
        "select range(d1.s2,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getLong(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange3() {
    String sqlStr =
        "select range(d1.s3,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getFloat(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testRange4() {
    String sqlStr =
        "select range(d1.s4,\"lower_bound\"=\"-5.0\",\"upper_bound\"=\"5.0\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      Assert.assertEquals(10.0, result1, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testTwoSidedFileter1() {
    String sqlStr = "select TwoSidedFilter(d6.s1, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getInt(2);
      resultSet.next();
      double result2 = resultSet.getInt(2);
      resultSet.next();
      double result3 = resultSet.getInt(2);
      resultSet.next();
      double result4 = resultSet.getInt(2);
      resultSet.next();
      double result5 = resultSet.getInt(2);
      resultSet.next();
      double result6 = resultSet.getInt(2);
      resultSet.next();
      double result7 = resultSet.getInt(2);
      resultSet.next();
      double result8 = resultSet.getInt(2);
      resultSet.next();
      double result9 = resultSet.getInt(2);
      resultSet.next();
      double result10 = resultSet.getInt(2);
      resultSet.next();
      double result11 = resultSet.getInt(2);
      resultSet.next();
      double result12 = resultSet.getInt(2);
      resultSet.next();
      double result13 = resultSet.getInt(2);
      resultSet.next();
      double result14 = resultSet.getInt(2);
      resultSet.next();
      double result15 = resultSet.getInt(2);
      resultSet.next();
      double result16 = resultSet.getInt(2);
      resultSet.next();
      double result17 = resultSet.getInt(2);
      resultSet.next();
      double result18 = resultSet.getInt(2);
      resultSet.next();
      double result19 = resultSet.getInt(2);
      resultSet.next();
      double result20 = resultSet.getInt(2);
      resultSet.next();
      double result21 = resultSet.getInt(2);
      resultSet.next();
      double result22 = resultSet.getInt(2);
      resultSet.next();
      double result23 = resultSet.getInt(2);
      Assert.assertEquals(2002.0, result1, 0.01);
      Assert.assertEquals(1946.0, result2, 0.01);
      Assert.assertEquals(1958.0, result3, 0.01);
      Assert.assertEquals(2012.0, result4, 0.01);
      Assert.assertEquals(2051.0, result5, 0.01);
      Assert.assertEquals(1898.0, result6, 0.01);
      Assert.assertEquals(2014.0, result7, 0.01);
      Assert.assertEquals(2052.0, result8, 0.01);
      Assert.assertEquals(1935.0, result9, 0.01);
      Assert.assertEquals(1901.0, result10, 0.01);
      Assert.assertEquals(1972.0, result11, 0.01);
      Assert.assertEquals(1969.0, result12, 0.01);
      Assert.assertEquals(1984.0, result13, 0.01);
      Assert.assertEquals(2018.0, result14, 0.01);
      Assert.assertEquals(1023.0, result15, 0.01);
      Assert.assertEquals(1056.0, result16, 0.01);
      Assert.assertEquals(978.0, result17, 0.01);
      Assert.assertEquals(1050.0, result18, 0.01);
      Assert.assertEquals(1123.0, result19, 0.01);
      Assert.assertEquals(1150.0, result20, 0.01);
      Assert.assertEquals(1134.0, result21, 0.01);
      Assert.assertEquals(950.0, result22, 0.01);
      Assert.assertEquals(1059.0, result23, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testTwoSidedFileter2() {
    String sqlStr = "select TwoSidedFilter(d6.s2, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getLong(2);
      resultSet.next();
      double result2 = resultSet.getLong(2);
      resultSet.next();
      double result3 = resultSet.getLong(2);
      resultSet.next();
      double result4 = resultSet.getLong(2);
      resultSet.next();
      double result5 = resultSet.getLong(2);
      resultSet.next();
      double result6 = resultSet.getLong(2);
      resultSet.next();
      double result7 = resultSet.getLong(2);
      resultSet.next();
      double result8 = resultSet.getLong(2);
      resultSet.next();
      double result9 = resultSet.getLong(2);
      resultSet.next();
      double result10 = resultSet.getLong(2);
      resultSet.next();
      double result11 = resultSet.getLong(2);
      resultSet.next();
      double result12 = resultSet.getLong(2);
      resultSet.next();
      double result13 = resultSet.getLong(2);
      resultSet.next();
      double result14 = resultSet.getLong(2);
      resultSet.next();
      double result15 = resultSet.getLong(2);
      resultSet.next();
      double result16 = resultSet.getLong(2);
      resultSet.next();
      double result17 = resultSet.getLong(2);
      resultSet.next();
      double result18 = resultSet.getLong(2);
      resultSet.next();
      double result19 = resultSet.getLong(2);
      resultSet.next();
      double result20 = resultSet.getLong(2);
      resultSet.next();
      double result21 = resultSet.getLong(2);
      resultSet.next();
      double result22 = resultSet.getLong(2);
      resultSet.next();
      double result23 = resultSet.getLong(2);
      Assert.assertEquals(2002.0, result1, 0.01);
      Assert.assertEquals(1946.0, result2, 0.01);
      Assert.assertEquals(1958.0, result3, 0.01);
      Assert.assertEquals(2012.0, result4, 0.01);
      Assert.assertEquals(2051.0, result5, 0.01);
      Assert.assertEquals(1898.0, result6, 0.01);
      Assert.assertEquals(2014.0, result7, 0.01);
      Assert.assertEquals(2052.0, result8, 0.01);
      Assert.assertEquals(1935.0, result9, 0.01);
      Assert.assertEquals(1901.0, result10, 0.01);
      Assert.assertEquals(1972.0, result11, 0.01);
      Assert.assertEquals(1969.0, result12, 0.01);
      Assert.assertEquals(1984.0, result13, 0.01);
      Assert.assertEquals(2018.0, result14, 0.01);
      Assert.assertEquals(1023.0, result15, 0.01);
      Assert.assertEquals(1056.0, result16, 0.01);
      Assert.assertEquals(978.0, result17, 0.01);
      Assert.assertEquals(1050.0, result18, 0.01);
      Assert.assertEquals(1123.0, result19, 0.01);
      Assert.assertEquals(1150.0, result20, 0.01);
      Assert.assertEquals(1134.0, result21, 0.01);
      Assert.assertEquals(950.0, result22, 0.01);
      Assert.assertEquals(1059.0, result23, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testTwoSidedFileter3() {
    String sqlStr = "select TwoSidedFilter(d6.s3, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getFloat(2);
      resultSet.next();
      double result2 = resultSet.getFloat(2);
      resultSet.next();
      double result3 = resultSet.getFloat(2);
      resultSet.next();
      double result4 = resultSet.getFloat(2);
      resultSet.next();
      double result5 = resultSet.getFloat(2);
      resultSet.next();
      double result6 = resultSet.getFloat(2);
      resultSet.next();
      double result7 = resultSet.getFloat(2);
      resultSet.next();
      double result8 = resultSet.getFloat(2);
      resultSet.next();
      double result9 = resultSet.getFloat(2);
      resultSet.next();
      double result10 = resultSet.getFloat(2);
      resultSet.next();
      double result11 = resultSet.getFloat(2);
      resultSet.next();
      double result12 = resultSet.getFloat(2);
      resultSet.next();
      double result13 = resultSet.getFloat(2);
      resultSet.next();
      double result14 = resultSet.getFloat(2);
      resultSet.next();
      double result15 = resultSet.getFloat(2);
      resultSet.next();
      double result16 = resultSet.getFloat(2);
      resultSet.next();
      double result17 = resultSet.getFloat(2);
      resultSet.next();
      double result18 = resultSet.getFloat(2);
      resultSet.next();
      double result19 = resultSet.getFloat(2);
      resultSet.next();
      double result20 = resultSet.getFloat(2);
      resultSet.next();
      double result21 = resultSet.getFloat(2);
      resultSet.next();
      double result22 = resultSet.getFloat(2);
      resultSet.next();
      double result23 = resultSet.getFloat(2);
      Assert.assertEquals(2002.0, result1, 0.01);
      Assert.assertEquals(1946.0, result2, 0.01);
      Assert.assertEquals(1958.0, result3, 0.01);
      Assert.assertEquals(2012.0, result4, 0.01);
      Assert.assertEquals(2051.0, result5, 0.01);
      Assert.assertEquals(1898.0, result6, 0.01);
      Assert.assertEquals(2014.0, result7, 0.01);
      Assert.assertEquals(2052.0, result8, 0.01);
      Assert.assertEquals(1935.0, result9, 0.01);
      Assert.assertEquals(1901.0, result10, 0.01);
      Assert.assertEquals(1972.0, result11, 0.01);
      Assert.assertEquals(1969.0, result12, 0.01);
      Assert.assertEquals(1984.0, result13, 0.01);
      Assert.assertEquals(2018.0, result14, 0.01);
      Assert.assertEquals(1023.0, result15, 0.01);
      Assert.assertEquals(1056.0, result16, 0.01);
      Assert.assertEquals(978.0, result17, 0.01);
      Assert.assertEquals(1050.0, result18, 0.01);
      Assert.assertEquals(1123.0, result19, 0.01);
      Assert.assertEquals(1150.0, result20, 0.01);
      Assert.assertEquals(1134.0, result21, 0.01);
      Assert.assertEquals(950.0, result22, 0.01);
      Assert.assertEquals(1059.0, result23, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Ignore // TODO: This test case failed, please check the function implementation
  @Test
  public void testTwoSidedFileter4() {
    String sqlStr = "select TwoSidedFilter(d6.s4, 'len'='3', 'threshold'='0.3') from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      resultSet.next();
      double result3 = resultSet.getDouble(2);
      resultSet.next();
      double result4 = resultSet.getDouble(2);
      resultSet.next();
      double result5 = resultSet.getDouble(2);
      resultSet.next();
      double result6 = resultSet.getDouble(2);
      resultSet.next();
      double result7 = resultSet.getDouble(2);
      resultSet.next();
      double result8 = resultSet.getDouble(2);
      resultSet.next();
      double result9 = resultSet.getDouble(2);
      resultSet.next();
      double result10 = resultSet.getDouble(2);
      resultSet.next();
      double result11 = resultSet.getDouble(2);
      resultSet.next();
      double result12 = resultSet.getDouble(2);
      resultSet.next();
      double result13 = resultSet.getDouble(2);
      resultSet.next();
      double result14 = resultSet.getDouble(2);
      resultSet.next();
      double result15 = resultSet.getDouble(2);
      resultSet.next();
      double result16 = resultSet.getDouble(2);
      resultSet.next();
      double result17 = resultSet.getDouble(2);
      resultSet.next();
      double result18 = resultSet.getDouble(2);
      resultSet.next();
      double result19 = resultSet.getDouble(2);
      resultSet.next();
      double result20 = resultSet.getDouble(2);
      resultSet.next();
      double result21 = resultSet.getDouble(2);
      resultSet.next();
      double result22 = resultSet.getDouble(2);
      resultSet.next();
      double result23 = resultSet.getDouble(2);
      Assert.assertEquals(2002.0, result1, 0.01);
      Assert.assertEquals(1946.0, result2, 0.01);
      Assert.assertEquals(1958.0, result3, 0.01);
      Assert.assertEquals(2012.0, result4, 0.01);
      Assert.assertEquals(2051.0, result5, 0.01);
      Assert.assertEquals(1898.0, result6, 0.01);
      Assert.assertEquals(2014.0, result7, 0.01);
      Assert.assertEquals(2052.0, result8, 0.01);
      Assert.assertEquals(1935.0, result9, 0.01);
      Assert.assertEquals(1901.0, result10, 0.01);
      Assert.assertEquals(1972.0, result11, 0.01);
      Assert.assertEquals(1969.0, result12, 0.01);
      Assert.assertEquals(1984.0, result13, 0.01);
      Assert.assertEquals(2018.0, result14, 0.01);
      Assert.assertEquals(1023.0, result15, 0.01);
      Assert.assertEquals(1056.0, result16, 0.01);
      Assert.assertEquals(978.0, result17, 0.01);
      Assert.assertEquals(1050.0, result18, 0.01);
      Assert.assertEquals(1123.0, result19, 0.01);
      Assert.assertEquals(1150.0, result20, 0.01);
      Assert.assertEquals(1134.0, result21, 0.01);
      Assert.assertEquals(950.0, result22, 0.01);
      Assert.assertEquals(1059.0, result23, 0.01);
      Assert.assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void testOutlier1() {
    String sqlStr =
        "select outlier(d0.s0, \"r\"=\"5\", \"k\"=\"4\", \"w\"=\"10\", \"s\"=\"5\") from root.vehicle";
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      ResultSet resultSet = statement.executeQuery(sqlStr);
      resultSet.next();
      double result1 = resultSet.getDouble(2);
      resultSet.next();
      double result2 = resultSet.getDouble(2);
      Assert.assertEquals(69.0, result1, 0.01);
      Assert.assertEquals(52.0, result2, 0.01);
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }
}
