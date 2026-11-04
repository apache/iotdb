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

package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.QUERY_ID;
import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.STATEMENT;
import static org.apache.iotdb.db.it.utils.TestUtils.createUser;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSeriesPrivilege;
import static org.apache.iotdb.db.it.utils.TestUtils.grantUserSystemPrivileges;
import static org.apache.iotdb.rpc.TSStatusCode.NO_PERMISSION;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowQueriesIT {

  private static final String DATABASE = "root.show_queries";
  private static final String DEVICE = DATABASE + ".d1";
  private static final String TIMESERIES = DEVICE + ".s1";
  private static final String QUERY_USER = "show_queries_query_user";
  private static final String SHOW_USER = "show_queries_show_user";
  private static final String KILL_USER = "show_queries_kill_user";
  private static final String PASSWORD = "password123456";
  private static final String PENDING_QUERY = "SELECT s1 FROM " + DEVICE;
  // Leave one default 1,000-row TsBlock after the initial Session fetch.
  private static final int STREAMING_QUERY_ROW_COUNT = SessionConfig.DEFAULT_FETCH_SIZE + 1_000;

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnforceStrongPassword(false);
    EnvFactory.getEnv().initClusterEnvironment();
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      session.executeNonQueryStatement("CREATE TIMESERIES " + TIMESERIES + " INT32");
      Tablet tablet =
          new Tablet(
              DEVICE,
              List.of(new MeasurementSchema("s1", TSDataType.INT32)),
              STREAMING_QUERY_ROW_COUNT);
      for (int i = 0; i < STREAMING_QUERY_ROW_COUNT; i++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, i);
        tablet.addValue("s1", rowIndex, i);
      }
      session.insertTablet(tablet);
    }
    createUser(QUERY_USER, PASSWORD);
    createUser(SHOW_USER, PASSWORD);
    createUser(KILL_USER, PASSWORD);
    grantUserSeriesPrivilege(QUERY_USER, PrivilegeType.READ_DATA, DATABASE + ".**");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void showQueriesAuthTest() throws Exception {
    try (ISession querySession = EnvFactory.getEnv().getSessionConnection(QUERY_USER, PASSWORD);
        ISession showQueriesSession =
            EnvFactory.getEnv().getSessionConnection(SHOW_USER, PASSWORD);
        SessionDataSet pendingDataSet = querySession.executeQueryStatement(PENDING_QUERY)) {
      // Keep QUERY_USER's query active by not consuming pendingDataSet.
      Assert.assertFalse(containsQuery(showQueriesSession, PENDING_QUERY));

      grantUserSystemPrivileges(SHOW_USER, PrivilegeType.SYSTEM);
      Assert.assertTrue(containsQuery(showQueriesSession, PENDING_QUERY));
    }
  }

  @Test
  public void killQueryAuthTest() throws Exception {
    try (ISession querySession = EnvFactory.getEnv().getSessionConnection(QUERY_USER, PASSWORD);
        ISession killSession = EnvFactory.getEnv().getSessionConnection(KILL_USER, PASSWORD);
        ISession adminSession = EnvFactory.getEnv().getSessionConnection();
        SessionDataSet pendingDataSet = querySession.executeQueryStatement(PENDING_QUERY)) {
      // Keep QUERY_USER's query active by not consuming pendingDataSet.
      String queryId = getQueryId(adminSession, PENDING_QUERY);

      StatementExecutionException exception =
          Assert.assertThrows(
              StatementExecutionException.class,
              () -> killSession.executeNonQueryStatement("KILL QUERY '" + queryId + "'"));
      Assert.assertEquals(NO_PERMISSION.getStatusCode(), exception.getStatusCode());
      Assert.assertEquals(queryId, getQueryId(adminSession, PENDING_QUERY));

      grantUserSystemPrivileges(KILL_USER, PrivilegeType.SYSTEM);
      killSession.executeNonQueryStatement("KILL QUERY '" + queryId + "'");
    }
  }

  private static boolean containsQuery(ISession session, String query) throws Exception {
    try (SessionDataSet dataSet = session.executeQueryStatement("SHOW QUERIES")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        if (query.equals(iterator.getString(STATEMENT))) {
          return true;
        }
      }
      return false;
    }
  }

  private static String getQueryId(ISession session, String query) throws Exception {
    try (SessionDataSet dataSet = session.executeQueryStatement("SHOW QUERIES")) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        if (query.equals(iterator.getString(STATEMENT))) {
          return iterator.getString(QUERY_ID);
        }
      }
    }
    Assert.fail("Query not found: " + query);
    return null;
  }
}
