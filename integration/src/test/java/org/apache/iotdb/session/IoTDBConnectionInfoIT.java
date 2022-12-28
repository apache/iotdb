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
package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBConnectionInfoIT {

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testGetBackupConfiguration() throws IoTDBConnectionException {
    Session session1 = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    Session session2 = new Session("127.0.0.1", 6667, "root", "root", ZoneId.of("+05:00"));
    try {
      session1.open();
      session2.open();
      TSConnectionInfoResp resp1 = session1.fetchAllConnections();
      TSConnectionInfoResp resp2 = session2.fetchAllConnections();
      assertEquals(2, resp1.connectionInfoList.size());
      assertEquals("root", resp1.connectionInfoList.get(0).userName);
      assertEquals("root", resp1.connectionInfoList.get(1).userName);
      assertEquals(TSConnectionType.THRIFT_BASED, resp1.connectionInfoList.get(0).type);
      assertEquals(TSConnectionType.THRIFT_BASED, resp1.connectionInfoList.get(1).type);
      assertTrue(resp1.connectionInfoList.get(0).connectionId.startsWith("127.0.0.1"));
      assertTrue(resp1.connectionInfoList.get(1).connectionId.startsWith("127.0.0.1"));

      assertEquals(2, resp2.connectionInfoList.size());
      assertEquals("root", resp2.connectionInfoList.get(0).userName);
      assertEquals("root", resp2.connectionInfoList.get(1).userName);
      assertEquals(TSConnectionType.THRIFT_BASED, resp2.connectionInfoList.get(0).type);
      assertEquals(TSConnectionType.THRIFT_BASED, resp2.connectionInfoList.get(1).type);
      assertTrue(resp2.connectionInfoList.get(0).connectionId.startsWith("127.0.0.1"));
      assertTrue(resp2.connectionInfoList.get(1).connectionId.startsWith("127.0.0.1"));

      assertEquals(
          resp1.connectionInfoList.get(0).connectionId,
          resp2.connectionInfoList.get(0).connectionId);
      assertEquals(
          resp1.connectionInfoList.get(1).connectionId,
          resp2.connectionInfoList.get(1).connectionId);
      assertEquals(
          resp1.connectionInfoList.get(0).logInTime, resp2.connectionInfoList.get(0).logInTime);
      assertEquals(
          resp1.connectionInfoList.get(1).logInTime, resp2.connectionInfoList.get(1).logInTime);

    } catch (Exception e) {
      fail();
    } finally {
      session1.close();
      session2.close();
    }
  }
}
