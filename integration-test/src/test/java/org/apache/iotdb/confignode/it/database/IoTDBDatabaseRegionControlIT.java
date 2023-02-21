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

package org.apache.iotdb.confignode.it.database;

import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionResp;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBDatabaseRegionControlIT {

  private static final int testDefaultSchemaRegionGroupNumPerDatabase = 1;
  private static final int testDefaultDataRegionGroupNumPerDatabase = 2;

  private static final int batchSize = 10;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDefaultSchemaRegionGroupNumPerDatabase(testDefaultSchemaRegionGroupNumPerDatabase)
        .setDefaultDataRegionGroupNumPerDatabase(testDefaultDataRegionGroupNumPerDatabase);

    // Init 1C1D environment
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void insertBatchData(Statement statement, String database, int tStart)
      throws SQLException {
    for (int i = tStart; i < tStart + batchSize; i++) {
      final String insertSQL =
          String.format("INSERT INTO %s.tsito%d(timestamp, s) values(1, 1);", database, i);
      statement.execute(insertSQL);
    }
  }

  @Test
  public void testDefaultRegionGroupNumControl()
      throws SQLException, ClientManagerException, IOException, InterruptedException, TException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient configNodeClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Initialize test data
      final String database = "root.paradise0";
      String createDatabaseSQL = String.format("CREATE DATABASE %s;", database);
      statement.execute(createDatabaseSQL);
      insertBatchData(statement, database, 0);

      // Check RegionGroupNum, which should be equal to the default value
      TShowRegionResp resp =
          configNodeClient.showRegion(
              new TShowRegionReq().setDatabases(Collections.singletonList(database)));
      AtomicInteger schemaRegionGroupNum = new AtomicInteger(0);
      AtomicInteger dataRegionGroupNum = new AtomicInteger(0);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
      resp.getRegionInfoList()
          .forEach(
              regionInfo -> {
                switch (regionInfo.getConsensusGroupId().getType()) {
                  case SchemaRegion:
                    schemaRegionGroupNum.getAndIncrement();
                    break;
                  case DataRegion:
                  default:
                    dataRegionGroupNum.getAndIncrement();
                    break;
                }
              });
      Assert.assertEquals(testDefaultSchemaRegionGroupNumPerDatabase, schemaRegionGroupNum.get());
      Assert.assertEquals(testDefaultDataRegionGroupNumPerDatabase, dataRegionGroupNum.get());
    }
  }

  @Test
  public void testRegionGroupNumControlThroughCreation()
      throws SQLException, ClientManagerException, IOException, InterruptedException, TException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient configNodeClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Initialize test data
      final String database = "root.paradise1";
      final int testSchemaRegionGroupNum = 2;
      final int testDataRegionGroupNum = 3;
      String createDatabaseSQL =
          String.format(
              "CREATE DATABASE %s WITH SCHEMA_REGION_GROUP_NUM=%d, DATA_REGION_GROUP_NUM=%d;",
              database, testSchemaRegionGroupNum, testDataRegionGroupNum);
      statement.execute(createDatabaseSQL);
      insertBatchData(statement, database, 0);

      // Check RegionGroupNum, which should be equal to the specified value
      TShowRegionResp resp =
          configNodeClient.showRegion(
              new TShowRegionReq().setDatabases(Collections.singletonList(database)));
      AtomicInteger schemaRegionGroupNum = new AtomicInteger(0);
      AtomicInteger dataRegionGroupNum = new AtomicInteger(0);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
      resp.getRegionInfoList()
          .forEach(
              regionInfo -> {
                switch (regionInfo.getConsensusGroupId().getType()) {
                  case SchemaRegion:
                    schemaRegionGroupNum.getAndIncrement();
                    break;
                  case DataRegion:
                  default:
                    dataRegionGroupNum.getAndIncrement();
                    break;
                }
              });
      Assert.assertEquals(testSchemaRegionGroupNum, schemaRegionGroupNum.get());
      Assert.assertEquals(testDataRegionGroupNum, dataRegionGroupNum.get());
    }
  }

  @Test
  public void testRegionGroupNumControlThroughAlter()
      throws SQLException, ClientManagerException, IOException, InterruptedException, TException {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement();
        final SyncConfigNodeIServiceClient configNodeClient =
            (SyncConfigNodeIServiceClient) EnvFactory.getEnv().getLeaderConfigNodeConnection()) {

      // Initialize test data
      final String database = "root.paradise2";
      String createDatabaseSQL = String.format("CREATE DATABASE %s;", database);
      statement.execute(createDatabaseSQL);
      insertBatchData(statement, database, 0);

      // Check RegionGroupNum, which should be equal to the default value
      TShowRegionResp resp =
          configNodeClient.showRegion(
              new TShowRegionReq().setDatabases(Collections.singletonList(database)));
      AtomicInteger schemaRegionGroupNum = new AtomicInteger(0);
      AtomicInteger dataRegionGroupNum = new AtomicInteger(0);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
      resp.getRegionInfoList()
          .forEach(
              regionInfo -> {
                switch (regionInfo.getConsensusGroupId().getType()) {
                  case SchemaRegion:
                    schemaRegionGroupNum.getAndIncrement();
                    break;
                  case DataRegion:
                  default:
                    dataRegionGroupNum.getAndIncrement();
                    break;
                }
              });
      Assert.assertEquals(testDefaultSchemaRegionGroupNumPerDatabase, schemaRegionGroupNum.get());
      Assert.assertEquals(testDefaultDataRegionGroupNumPerDatabase, dataRegionGroupNum.get());

      // Alter DatabaseSchema
      final int testSchemaRegionGroupNum = 3;
      final int testDataRegionGroupNum = 3;
      String alterDatabaseSQL =
          String.format(
              "ALTER DATABASE %s WITH SCHEMA_REGION_GROUP_NUM=%d, DATA_REGION_GROUP_NUM=%d;",
              database, testSchemaRegionGroupNum, testDataRegionGroupNum);
      statement.execute(alterDatabaseSQL);
      insertBatchData(statement, database, batchSize);

      // Check RegionGroupNum, which should be equal to the specified value
      resp =
          configNodeClient.showRegion(
              new TShowRegionReq().setDatabases(Collections.singletonList(database)));
      schemaRegionGroupNum.set(0);
      dataRegionGroupNum.set(0);
      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), resp.getStatus().getCode());
      resp.getRegionInfoList()
          .forEach(
              regionInfo -> {
                switch (regionInfo.getConsensusGroupId().getType()) {
                  case SchemaRegion:
                    schemaRegionGroupNum.getAndIncrement();
                    break;
                  case DataRegion:
                  default:
                    dataRegionGroupNum.getAndIncrement();
                    break;
                }
              });
      Assert.assertEquals(testSchemaRegionGroupNum, schemaRegionGroupNum.get());
      Assert.assertEquals(testDataRegionGroupNum, dataRegionGroupNum.get());
    }
  }
}
