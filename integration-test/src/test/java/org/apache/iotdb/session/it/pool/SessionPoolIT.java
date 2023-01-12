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
package org.apache.iotdb.session.it.pool;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.pool.ISessionPool;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// this test is not for testing the correctness of Session API. So we just implement one of the API.
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class SessionPoolIT {

  private static final Logger logger = LoggerFactory.getLogger(SessionPoolIT.class);
  private static final long DEFAULT_QUERY_TIMEOUT = -1;

  @Before
  public void setUp() throws Exception {
    // As this IT is only testing SessionPool itself, there's no need to launch a large cluster
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void insert() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(
          () -> {
            try {
              pool.insertRecord(
                  "root.sg1.d1",
                  1,
                  Collections.singletonList("s" + no),
                  Collections.singletonList(TSDataType.INT64),
                  Collections.singletonList(3L));
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              fail(e.getMessage());
            }
          });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.currentAvailableSize() <= 3);
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      logger.error("insert failed", e);
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void incorrectSQL() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    assertEquals(0, pool.currentAvailableSize());
    try {
      pool.insertRecord(
          ".root.sg1.d1",
          1,
          Collections.singletonList("s"),
          Collections.singletonList(TSDataType.INT64),
          Collections.singletonList(3L));
      assertEquals(1, pool.currentAvailableSize());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      // do nothing
    } finally {
      pool.close();
    }
  }

  @Test
  public void incorrectExecuteQueryStatement() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    write10Data(pool, true);
    // now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(
          () -> {
            try {
              SessionDataSetWrapper wrapper =
                  pool.executeQueryStatement("select * from root.sg1.d1 where time = " + no);
              // this is incorrect because wrapper is not closed.
              // so all other 7 queries will be blocked
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              fail(e.getMessage());
            }
          });
    }
    service.shutdown();
    try {
      assertFalse(service.awaitTermination(3, TimeUnit.SECONDS));
      assertEquals(0, pool.currentAvailableSize());
      assertTrue(pool.currentOccupiedSize() <= 3);
    } catch (InterruptedException e) {
      logger.error("incorrectExecuteQueryStatement failed,", e);
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void executeQueryStatement() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    correctQuery(pool, DEFAULT_QUERY_TIMEOUT);
    pool.close();
  }

  @Test
  public void executeQueryStatementWithTimeout() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    correctQuery(pool, 2000);
    pool.close();
  }

  private void correctQuery(ISessionPool pool, long timeoutInMs) {
    ExecutorService service = Executors.newFixedThreadPool(10);
    write10Data(pool, true);
    // now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(
          () -> {
            try {
              SessionDataSetWrapper wrapper;
              if (timeoutInMs == DEFAULT_QUERY_TIMEOUT) {
                wrapper =
                    pool.executeQueryStatement("select * from root.sg1.d1 where time = " + no);
              } else {
                wrapper =
                    pool.executeQueryStatement(
                        "select * from root.sg1.d1 where time = " + no, timeoutInMs);
              }
              pool.closeResultSet(wrapper);
            } catch (Exception e) {
              logger.error("correctQuery failed", e);
              fail(e.getMessage());
            }
          });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.currentAvailableSize() <= 3);
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      logger.error("correctQuery failed", e);
      fail(e.getMessage());
    }
  }

  @Test
  public void executeRawDataQuery() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    write10Data(pool, true);
    List<String> pathList = new ArrayList<>();
    pathList.add("root.sg1.d1.s1");
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(
          () -> {
            try {
              SessionDataSetWrapper wrapper = pool.executeRawDataQuery(pathList, no, no + 1, 60000);
              if (wrapper.hasNext()) {
                assertEquals(no, wrapper.next().getTimestamp());
              }
              pool.closeResultSet(wrapper);
            } catch (Exception e) {
              logger.error("executeRawDataQuery", e);
              fail(e.getMessage());
            }
          });
    }
    try {
      service.shutdown();
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertTrue(pool.currentAvailableSize() <= 3);
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void tryIfTheServerIsRestart() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    SessionDataSetWrapper wrapper = null;
    BaseNodeWrapper node = EnvFactory.getEnv().getDataNodeWrapper(0);
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      node.stop();
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(node), Collections.singletonList(NodeStatus.Unknown));
      // user does not know what happens.
      while (wrapper.hasNext()) {
        wrapper.next();
      }
    } catch (IoTDBConnectionException e) {
      pool.closeResultSet(wrapper);
      pool.close();
      node.stop();
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(node), Collections.singletonList(NodeStatus.Unknown));
      node.start();
      EnvFactory.getEnv()
          .ensureNodeStatus(
              Collections.singletonList(node), Collections.singletonList(NodeStatus.Running));
      pool = EnvFactory.getEnv().getSessionPool(3);
      correctQuery(pool, DEFAULT_QUERY_TIMEOUT);
      pool.close();
      return;
    } catch (StatementExecutionException e) {
      // After receiving the stop request, thrift calls shutdownNow() to process the executing task.
      // However, when the executing task is blocked, it will report InterruptedException error.
      // And IoTDB warps it as one StatementExecutionException.
      // If the thrift task thread is running, the thread will not be affected and will continue to
      // run, only if the interrupt flag of the thread is set to true. So here, we call the close
      // function on the client and wait for some time before the thrift server can exit normally.
      try {
        while (wrapper.hasNext()) {
          wrapper.next();
        }
      } catch (IoTDBConnectionException ec) {
        pool.closeResultSet(wrapper);
        pool.close();
        node.stop();
        EnvFactory.getEnv()
            .ensureNodeStatus(
                Collections.singletonList(node), Collections.singletonList(NodeStatus.Unknown));
        node.start();
        EnvFactory.getEnv()
            .ensureNodeStatus(
                Collections.singletonList(node), Collections.singletonList(NodeStatus.Running));
        pool = EnvFactory.getEnv().getSessionPool(3);
        correctQuery(pool, DEFAULT_QUERY_TIMEOUT);
        pool.close();
      } catch (StatementExecutionException es) {
        fail("should be TTransportException but get an exception: " + e.getMessage());
      }
      return;
    } finally {
      if (wrapper != null) {
        pool.closeResultSet(wrapper);
      }
      pool.close();
    }
    fail("should throw exception but not");
  }

  @Test
  public void tryIfTheServerIsRestartButDataIsGotten() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    write10Data(pool, true);
    assertEquals(1, pool.currentAvailableSize());
    SessionDataSetWrapper wrapper;
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      // user does not know what happens.
      assertEquals(0, pool.currentAvailableSize());
      assertEquals(1, pool.currentOccupiedSize());
      while (wrapper.hasNext()) {
        wrapper.next();
      }
      pool.closeResultSet(wrapper);
      assertEquals(1, pool.currentAvailableSize());
      assertEquals(0, pool.currentOccupiedSize());
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      logger.error("tryIfTheServerIsRestartButDataIsGotten", e);
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }

  @Test
  public void restart() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(1);
    write10Data(pool, true);
    // stop the server.
    pool.close();
    BaseNodeWrapper node = EnvFactory.getEnv().getDataNodeWrapper(0);
    node.stop();
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(node), Collections.singletonList(NodeStatus.Unknown));
    pool = EnvFactory.getEnv().getSessionPool(1);
    // all this ten data will fail.
    write10Data(pool, false);
    // restart the server
    node.start();
    EnvFactory.getEnv()
        .ensureNodeStatus(
            Collections.singletonList(node), Collections.singletonList(NodeStatus.Running));
    write10Data(pool, true);
    pool.close();
  }

  private void write10Data(ISessionPool pool, boolean failWhenThrowException) {
    for (int i = 0; i < 10; i++) {
      try {
        pool.insertRecord(
            "root.sg1.d1",
            i,
            Collections.singletonList("s" + i),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList((long) i));
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        // will fail this 10 times.
        if (failWhenThrowException) {
          fail(e.getMessage());
        }
      }
    }
  }

  @Test
  public void testClose() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    pool.close();
    try {
      pool.insertRecord(
          "root.sg1.d1",
          1,
          Collections.singletonList("s1"),
          Collections.singletonList(TSDataType.INT64),
          Collections.singletonList(1L));
    } catch (IoTDBConnectionException e) {
      assertEquals("Session pool is closed", e.getMessage());
    } catch (StatementExecutionException e) {
      fail(e.getMessage());
    }
    // some other test cases are not covered:
    // e.g., thread A created a new session, but not returned; thread B close the pool; A get the
    // session.
  }

  @Test
  public void testSetters() {
    ISessionPool pool = EnvFactory.getEnv().getSessionPool(3);
    try {
      pool.setEnableRedirection(true);
      assertTrue(pool.isEnableRedirection());
      pool.setEnableQueryRedirection(true);
      assertTrue(pool.isEnableQueryRedirection());
      pool.setTimeZone("GMT+8");
      assertEquals(ZoneId.of("GMT+8"), pool.getZoneId());
      pool.setVersion(SessionConfig.DEFAULT_VERSION);
      assertEquals(SessionConfig.DEFAULT_VERSION, pool.getVersion());
      pool.setQueryTimeout(12345);
      assertEquals(12345, pool.getQueryTimeout());
      pool.setFetchSize(16);
      assertEquals(16, pool.getFetchSize());
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      pool.close();
    }
  }
}
