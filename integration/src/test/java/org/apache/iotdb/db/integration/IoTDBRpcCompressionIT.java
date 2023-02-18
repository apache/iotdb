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
import org.apache.iotdb.integration.env.ConfigFactory;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@Category({LocalStandaloneTest.class})
public class IoTDBRpcCompressionIT {
  boolean rpcThriftCompression =
      IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable();
  boolean rpcAdvancedCompression =
      IoTDBDescriptor.getInstance().getConfig().isRpcAdvancedCompressionEnable();

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig().setRpcThriftCompressionEnable(rpcThriftCompression);
    ConfigFactory.getConfig().setRpcAdvancedCompressionEnable(rpcAdvancedCompression);
  }

  @Test
  public void testNoRpcCompression() throws SQLException, InterruptedException {
    EnvFactory.getEnv().initBeforeClass();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      doSomething(statement);
    }
  }

  @Test
  public void testRpcThriftCompression() throws SQLException, InterruptedException {
    ConfigFactory.getConfig().setRpcThriftCompressionEnable(true);
    EnvFactory.getEnv().initBeforeClass();
    // let JDBC use thrift rpc compression
    Config.rpcThriftCompressionEnable = true;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      doSomething(statement);
    } finally {
      Config.rpcThriftCompressionEnable = false;
    }
  }

  @Test
  public void testRpcAdvancedCompression() throws SQLException, InterruptedException {
    ConfigFactory.getConfig().setRpcAdvancedCompressionEnable(true);
    EnvFactory.getEnv().initBeforeClass();
    // let JDBC use thrift advanced compression
    RpcTransportFactory.setUseSnappy(true);
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      doSomething(statement);
    }
  }

  @Test
  public void testBothRpcCompression() throws SQLException, InterruptedException {
    ConfigFactory.getConfig().setRpcThriftCompressionEnable(true);
    ConfigFactory.getConfig().setRpcThriftCompressionEnable(true);
    EnvFactory.getEnv().initBeforeTest();
    // let JDBC use thrift rpc compression
    Config.rpcThriftCompressionEnable = true;
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      doSomething(statement);
    } finally {
      Config.rpcThriftCompressionEnable = false;
    }
  }

  private void doSomething(Statement statement) throws SQLException, InterruptedException {

    statement.execute("CREATE DATABASE root.demo");
    statement.execute("create timeseries root.demo.d1.s1 with datatype=INT64,encoding=RLE");
    statement.execute("create timeseries root.demo.d1.s2 with datatype=INT64,encoding=RLE");
    statement.execute("create timeseries root.demo.d1.s3 with datatype=INT64,encoding=RLE");
    statement.execute("insert into root.demo.d1(time,s1,s2) values(1,1,2)");
    statement.execute("flush");
    statement.execute("insert into root.demo.d1(time,s3) values(1,1)");
    statement.execute("flush");
    try (ResultSet set = statement.executeQuery("SELECT * FROM root.**")) {
      int cnt = 0;
      while (set.next()) {
        cnt++;
        assertEquals(1, set.getLong("root.demo.d1.s3"));
      }
      assertEquals(1, cnt);
    }
    Thread.sleep(1000);
    // before merge completes
    try (ResultSet set = statement.executeQuery("SELECT * FROM root.**")) {
      int cnt = 0;
      while (set.next()) {
        cnt++;
        assertEquals(1, set.getLong("root.demo.d1.s3"));
      }
      assertEquals(1, cnt);
    }

    // after merge completes
    statement.execute("DELETE FROM root.demo.d1.**");
  }
}
