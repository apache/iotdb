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

package org.apache.iotdb.relational.it.query.view.recent;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;

public class IoTDBTableViewQueryWithCachedDeviceIT {

  protected static final String DATABASE_NAME = "test";

  protected static String[] createTreeNonAlignedDataSqls = {
    "CREATE TIMESERIES root.db.battery.b1.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b1.current FLOAT",
    "INSERT INTO root.db.battery.b1(time, voltage, current) values (1, 1, 1)",
    "CREATE TIMESERIES root.db.battery.b2.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b2.current FLOAT",
    "INSERT INTO root.db.battery.b2(time, voltage, current) values (1, 2, 2)",
    "CREATE TIMESERIES root.db.battery.b3.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b3.current FLOAT",
    "INSERT INTO root.db.battery.b3(time, voltage, current) values (1, 3, 3)",
    "CREATE TIMESERIES root.db.battery.b4.voltage INT32",
    "CREATE TIMESERIES root.db.battery.b4.current FLOAT",
    "INSERT INTO root.db.battery.b4(time, voltage, current) values (1, 4, 4)",
    "FLUSH",
  };

  protected static String[] createTableSqls = {
    "CREATE DATABASE " + DATABASE_NAME,
    "USE " + DATABASE_NAME,
    "CREATE VIEW view1 (battery TAG, voltage INT32 FIELD, current FLOAT FIELD) as root.db.battery.**",
  };

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setSortBufferSize(128 * 1024);
    EnvFactory.getEnv().getConfig().getCommonConfig().setMaxTsBlockSizeInByte(4 * 1024);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(createTreeNonAlignedDataSqls);
    prepareTableData(createTableSqls);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test1() throws IoTDBConnectionException, StatementExecutionException {
    int count1 = 0;
    int count2 = 0;
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + DATABASE_NAME);
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select * from view1 where battery = 'b1' or battery = 'b2' or battery = 'b3'");
      while (sessionDataSet.hasNext()) {
        sessionDataSet.next();
        count1++;
      }
    }
    EnvFactory.getEnv().shutdownAllDataNodes();
    for (DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(dataNodeWrapper),
              Collections.singletonList(NodeStatus.Unknown));
    }
    EnvFactory.getEnv().startAllDataNodes();
    for (DataNodeWrapper dataNodeWrapper : EnvFactory.getEnv().getDataNodeWrapperList()) {
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(dataNodeWrapper),
              Collections.singletonList(NodeStatus.Running));
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement(
          "INSERT INTO root.db.battery.b3(time, voltage, current) aligned values (2, 1, 1)");
    }

    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("use " + DATABASE_NAME);
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              "select * from view1 where (battery = 'b1' or battery = 'b2' or battery = 'b3') and time = 1");
      while (sessionDataSet.hasNext()) {
        sessionDataSet.next();
        count2++;
      }
    }
    Assert.assertEquals(count1, count2);
  }
}
