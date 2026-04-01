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
package org.apache.iotdb.session.it;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.read.common.RowRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({ClusterIT.class})
public class IoTDBActiveCloseConnectionIT {

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testCloseOnWhiteListUpdated() {
    try (ISession session1 = EnvFactory.getEnv().getSessionConnection()) {
      try {
        session1.executeNonQueryStatement("SET CONFIGURATION 'white_ip_list'='1.2.3.4'");
      } catch (StatementExecutionException e) {
        if (!e.getMessage().contains("301")) {
          // ignore 301 caused by RemoteIT
          throw e;
        }
      }

      try {
        session1.executeNonQueryStatement("SET CONFIGURATION 'enable_white_list'='true'");
      } catch (StatementExecutionException e) {
        if (!e.getMessage().contains("301")) {
          // ignore 301 caused by RemoteIT
          throw e;
        }
      }

      session1.executeNonQueryStatement("Flush");
      fail();
    } catch (Exception e) {
      assertEquals(
          "802: Log in failed. Either you are not authorized or the session has timed out.",
          e.getMessage());
    }
  }

  @Test
  public void testCloseOnBlackListUpdated() {
    try (ISession session1 = EnvFactory.getEnv().getSessionConnection()) {
      try {
        session1.executeNonQueryStatement("SET CONFIGURATION 'black_ip_list'='127.0.0.1'");
      } catch (StatementExecutionException e) {
        if (!e.getMessage().contains("301")) {
          // ignore 301 caused by RemoteIT
          throw e;
        }
      }

      try {
        session1.executeNonQueryStatement("SET CONFIGURATION 'enable_black_list'='true'");
      } catch (StatementExecutionException e) {
        if (!e.getMessage().contains("301")) {
          // ignore 301 caused by RemoteIT
          throw e;
        }
      }

      session1.executeNonQueryStatement("Flush");
      fail();
    } catch (Exception e) {
      assertEquals(
          "802: Log in failed. Either you are not authorized or the session has timed out.",
          e.getMessage());
    }
  }

  @Test
  public void testCloseOnDropUser() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session1 = EnvFactory.getEnv().getSessionConnection()) {
      session1.executeNonQueryStatement("CREATE USER close_on_drop 'TimechoDB@2025'");

      try (ISession normalSession =
          EnvFactory.getEnv().getSessionConnection("close_on_drop", "TimechoDB@2025")) {
        session1.executeNonQueryStatement("DROP USER close_on_drop");
        try {
          normalSession.executeQueryStatement("FLUSH");
          fail("Connection not closed");
        } catch (Exception e) {
          assertEquals(
              "802: Log in failed. Either you are not authorized or the session has timed out.",
              e.getMessage());
        }
      }
    }
  }

  @Test
  public void testCloseOnAlterUser() {
    try (ISession session1 = EnvFactory.getEnv().getSessionConnection()) {
      session1.executeNonQueryStatement("CREATE USER close_on_drop 'TimechoDB@2025'");

      try (ISession normalSession =
          EnvFactory.getEnv().getSessionConnection("close_on_drop", "TimechoDB@2025")) {
        session1.executeNonQueryStatement("ALTER USER close_on_drop SET PASSWORD 'TimechoDB@2026'");
        normalSession.executeNonQueryStatement("FLUSH");
        fail("Connection not closed");
      }

    } catch (Exception e) {
      assertEquals(
          "802: Log in failed. Either you are not authorized or the session has timed out.",
          e.getMessage());
    }
  }

  @Test
  public void testCloseOnInactive() throws Exception {
    try (ITableSession session1 = EnvFactory.getEnv().getTableSessionConnection();
        ITableSession session2 = EnvFactory.getEnv().getTableSessionConnection()) {
      try {
        session1.executeNonQueryStatement("SET CONFIGURATION idle_session_timeout_in_minutes='1'");
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // enable remote cluster
        if (!e.getMessage().contains("Unable to find the configuration file")) {
          throw e;
        }
      }

      long waitTime = 35 * 1000;
      Awaitility.await()
          .atMost(10, TimeUnit.MINUTES)
          .pollDelay(waitTime, TimeUnit.MILLISECONDS)
          .pollInterval(waitTime, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                try (SessionDataSet sessionDataSet =
                    session2.executeQueryStatement(
                        "SELECT * FROM information_schema.connections")) {

                  System.out.println("Remaining connections:");
                  int cnt = 0;
                  while (sessionDataSet.hasNext()) {
                    RowRecord next = sessionDataSet.next();
                    cnt++;
                    System.out.println(next.toString());
                  }

                  // 1 connection from session1 (NodeFetcher)
                  // 2 connections from session2 (NodeFetcher and actual connection)
                  // session1's actual connection should be disconnected
                  return cnt == 3;
                }
              });
    } catch (Exception e) {
      // closing an actively closed session will throw an exception because the socket is closed
      if (!e.getMessage()
          .contains("Error occurs when closing session at server. Maybe server is down")) {
        throw e;
      }
    }
  }
}
