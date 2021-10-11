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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

public class IOTDBInsertAlignedValuesIT {
  private static Connection connection;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(true);
    Class.forName(Config.JDBC_DRIVER_NAME);
    connection =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
  }

  @After
  public void tearDown() throws Exception {
    close();
    EnvironmentUtils.cleanEnv();
  }

  private static void close() {
    if (Objects.nonNull(connection)) {
      try {
        connection.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  @Ignore // SQL of insert aligned record is not supported yet
  public void testInsertAlignedValues() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (4000, (true, 17.1))");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (5000, (true, 20.1))");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (6000, (true, 22))");
    st0.close();

    Statement st1 = connection.createStatement();

    ResultSet rs1 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));

    ResultSet rs2 = st1.executeQuery("select * from root.t1.wf01.wt01");
    rs2.next();
    Assert.assertEquals(4000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(17.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(5000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(20.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(6000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(22, rs2.getFloat(3), 0.1);
    st1.close();
  }

  @Test
  @Ignore // SQL of insert aligned record is not supported yet
  public void testInsertAlignedNullableValues() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (4000, (true, 17.1))");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (5000, (true, null))");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (6000, (NULL, 22))");
    st0.close();

    Statement st1 = connection.createStatement();

    ResultSet rs1 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));

    ResultSet rs2 = st1.executeQuery("select * from root.t1.wf01.wt01");
    rs2.next();
    Assert.assertEquals(4000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(17.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(5000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getObject(2));
    Assert.assertEquals(null, rs2.getObject(3));

    rs2.next();
    Assert.assertEquals(6000, rs2.getLong(1));
    Assert.assertEquals(null, rs2.getObject(2));
    Assert.assertEquals(22.0f, rs2.getObject(3));
    st1.close();
  }

  @Test
  @Ignore // SQL of insert aligned record is not supported yet
  public void testUpdatingAlignedValues() throws SQLException {
    Statement st0 = connection.createStatement();
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (4000, (true, 17.1))");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (5000, (true, null))");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (5000, (NULL, 20.1))");
    st0.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values (6000, (null, 22))");
    st0.close();

    Statement st1 = connection.createStatement();

    ResultSet rs1 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));
    rs1.next();
    Assert.assertEquals(true, rs1.getBoolean(2));
    rs1.next();
    Assert.assertEquals(null, rs1.getObject(2));

    ResultSet rs2 = st1.executeQuery("select * from root.t1.wf01.wt01");
    rs2.next();
    Assert.assertEquals(4000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getBoolean(2));
    Assert.assertEquals(17.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(5000, rs2.getLong(1));
    Assert.assertEquals(true, rs2.getObject(2));
    Assert.assertEquals(20.1, rs2.getFloat(3), 0.1);

    rs2.next();
    Assert.assertEquals(6000, rs2.getLong(1));
    Assert.assertEquals(null, rs2.getObject(2));
    Assert.assertEquals(22.0f, rs2.getObject(3));

    st1.execute("flush");
    ResultSet rs3 = st1.executeQuery("select status from root.t1.wf01.wt01");
    rs3.next();
    Assert.assertEquals(true, rs3.getBoolean(2));
    rs3.next();
    Assert.assertEquals(true, rs3.getBoolean(2));
    rs3.next();
    Assert.assertEquals(null, rs3.getObject(2));

    ResultSet rs4 = st1.executeQuery("select * from root.t1.wf01.wt01");
    rs4.next();
    Assert.assertEquals(4000, rs4.getLong(1));
    Assert.assertEquals(true, rs4.getBoolean(2));
    Assert.assertEquals(17.1, rs4.getFloat(3), 0.1);

    rs4.next();
    Assert.assertEquals(5000, rs4.getLong(1));
    Assert.assertEquals(true, rs4.getObject(2));
    Assert.assertEquals(20.1, rs4.getFloat(3), 0.1);

    rs4.next();
    Assert.assertEquals(6000, rs4.getLong(1));
    Assert.assertEquals(null, rs4.getObject(2));
    Assert.assertEquals(22.0f, rs4.getObject(3));
    st1.close();
  }

  @Test(expected = Exception.class)
  public void testInsertWithWrongMeasurementNum1() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute("insert into root.t1.wf01.wt01(time, (status, temperature)) values(11000, 100)");
  }

  @Test(expected = Exception.class)
  public void testInsertWithWrongMeasurementNum2() throws SQLException {
    Statement st1 = connection.createStatement();
    st1.execute(
        "insert into root.t1.wf01.wt01(time, (status, temperature)) values(11000, (100, 300, 400))");
  }
}
