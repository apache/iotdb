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
package org.apache.iotdb.session.pool;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

//this test is not for testing the correctness of Session API. So we just implement one of the API.
public class SessionPoolTest {

  @Before
  public void setUp() throws Exception {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void insert() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        try {
          pool.insert("root.sg1.d1", 1, Collections.singletonList("s" + no), Collections.singletonList("3"));
        } catch (IoTDBSessionException e) {
          fail();
        }
      });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
    assertEquals(3, pool.currentAvailableSize());
    assertEquals(0, pool.currentOccupiedSize());
  }

  @Test
  public void incorrectSQL() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    assertEquals(0, pool.currentAvailableSize());
    try {
      pool.insert(".root.sg1.d1", 1, Collections.singletonList("s" ), Collections.singletonList("3"));
    } catch (IoTDBSessionException e) {
      //do nothing
    }
    assertEquals(1, pool.currentAvailableSize());
  }


  @Test
  public void incorrectExecuteQueryStatement() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insert("root.sg1.d1", i, Collections.singletonList("s" + i), Collections.singletonList("" + i));
      } catch (IoTDBSessionException e) {
        fail();
      }
    }
    //now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        try {
          SessionDataSetWrapper wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time = " + no);
          //this is incorrect becasue wrapper is not closed.
          //so all other 7 queries will be blocked
        } catch (IoTDBSessionException e) {
          fail();
        } catch (IoTDBRPCException e) {
          e.printStackTrace();
        }
      });
    }
    service.shutdown();
    try {
      assertFalse(service.awaitTermination(3, TimeUnit.SECONDS));
      assertEquals(0, pool.currentAvailableSize());
      assertEquals(3, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void executeQueryStatement() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    correctQuery(pool);
  }

  private void correctQuery(SessionPool pool) {
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insert("root.sg1.d1", i, Collections.singletonList("s" + i), Collections.singletonList("" + i));
      } catch (IoTDBSessionException e) {
        fail();
      }
    }
    //now let's query
    for (int i = 0; i < 10; i++) {
      final int no = i;
      service.submit(() -> {
        try {
          SessionDataSetWrapper wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time = " + no);
          pool.closeResultSet(wrapper);
          pool.closeResultSet(wrapper);
        } catch (Exception e) {
          e.printStackTrace();
          fail();
        }
      });
    }
    service.shutdown();
    try {
      assertTrue(service.awaitTermination(10, TimeUnit.SECONDS));
      assertEquals(3, pool.currentAvailableSize());
      assertEquals(0, pool.currentOccupiedSize());
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void tryIfTheServerIsRestart() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1);
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insert("root.sg1.d1", i, Collections.singletonList("s" + i), Collections.singletonList("" + i));
      } catch (IoTDBSessionException e) {
        fail();
      }
    }
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      EnvironmentUtils.stopDaemon();
      //user does not know what happens.
      while(wrapper.hasNext()) {
        wrapper.next();
      }
    } catch (IoTDBRPCException e) {
      e.printStackTrace();
      fail();
    } catch (IoTDBSessionException e) {
      e.printStackTrace();
      fail();
    } catch (SQLException e) {
      if (e.getCause() instanceof TException) {
        try {
          pool.closeResultSet(wrapper);
        } catch (SQLException ex) {
          ex.printStackTrace();
          fail();
        }
      } else {
        fail("should be TTransportException but get an exception: " + e.getMessage());
      }
      EnvironmentUtils.reactiveDaemon();
      correctQuery(pool);
      return;
    }
    fail("should throw exception but not");
  }

  @Test
  public void tryIfTheServerIsRestartButDataIsGotten() {
    SessionPool pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3, 1);
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      try {
        pool.insert("root.sg1.d1", i, Collections.singletonList("s" + i), Collections.singletonList("" + i));
      } catch (IoTDBSessionException e) {
        fail();
      }
    }
    assertEquals(1, pool.currentAvailableSize());
    SessionDataSetWrapper wrapper = null;
    try {
      wrapper = pool.executeQueryStatement("select * from root.sg1.d1 where time > 1");
      //user does not know what happens.
      assertEquals(0, pool.currentAvailableSize());
      assertEquals(1, pool.currentOccupiedSize());
      while(wrapper.hasNext()) {
        wrapper.next();
      }
      assertEquals(1, pool.currentAvailableSize());
      assertEquals(0, pool.currentOccupiedSize());
    } catch (IoTDBRPCException e) {
      e.printStackTrace();
      fail();
    } catch (IoTDBSessionException e) {
      e.printStackTrace();
      fail();
    } catch (SQLException e) {
      e.printStackTrace();
      fail();
    }
  }

}