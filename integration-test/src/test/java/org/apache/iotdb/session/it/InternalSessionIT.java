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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.internal.session.InternalSessionBuilder;
import org.apache.iotdb.commons.client.internal.session.InternalSessionPoolBuilder;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.InternalSessionITUtils;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import org.apache.tsfile.enums.TSDataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class InternalSessionIT {

  private static final String DATABASE = "root.internal_session_it";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInternalSessionUsesInternalPort() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final INodeSupplier nodeSupplier = InternalSessionITUtils.createInternalNodeSupplier();

    try (Session session =
        new InternalSessionBuilder(nodeSupplier)
            .host(dataNode.getInternalAddress())
            .port(dataNode.getInternalPort())
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      session.open();
      try {
        session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
        session.executeNonQueryStatement(
            "CREATE TIMESERIES " + DATABASE + ".d1.s1 WITH DATATYPE=INT64, ENCODING=RLE");
        session.insertRecord(
            DATABASE + ".d1",
            1L,
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList(1L));
        session.insertRecord(
            DATABASE + ".d1",
            1L,
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList(1L));

        try (SessionDataSet dataSet =
            session.executeQueryStatement("SELECT s1 FROM " + DATABASE + ".d1")) {
          assertTrue(dataSet.hasNext());
          assertEquals(1L, dataSet.next().getFields().get(0).getLongV());
        }
      } finally {
        session.executeNonQueryStatement("DELETE DATABASE " + DATABASE);
      }
    }
  }

  @Test
  public void testInternalEndpointServesDefaultAndClientServices() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final TEndPoint internalEndPoint = InternalSessionITUtils.getInternalEndPoint(dataNode);

    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try (final SyncDataNodeInternalServiceClient client = clients.borrowClient(internalEndPoint)) {
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.testConnectionEmptyRPC().getCode());
    } finally {
      clients.close();
    }

    final INodeSupplier nodeSupplier = InternalSessionITUtils.createInternalNodeSupplier();
    try (Session session =
        new InternalSessionBuilder(nodeSupplier)
            .host(dataNode.getInternalAddress())
            .port(dataNode.getInternalPort())
            .username(SessionConfig.DEFAULT_USER)
            .build()) {
      session.open();
      try (SessionDataSet dataSet = session.executeQueryStatement("SHOW DATABASES")) {
        assertNotNull(dataSet);
      }
    }
  }

  @Test
  public void testInternalLoginRejectsUnknownUser() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final INodeSupplier nodeSupplier = InternalSessionITUtils.createInternalNodeSupplier();

    try (Session session =
        new InternalSessionBuilder(nodeSupplier)
            .host(dataNode.getInternalAddress())
            .port(dataNode.getInternalPort())
            .username("missing_internal_session_user")
            .build()) {
      assertThrows(IoTDBConnectionException.class, session::open);
    }
  }

  @Test
  public void testInternalSessionPoolUsesInternalPort() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final INodeSupplier nodeSupplier = InternalSessionITUtils.createInternalNodeSupplier();

    final SessionPool sessionPool =
        new InternalSessionPoolBuilder(nodeSupplier)
            .host(dataNode.getInternalAddress())
            .port(dataNode.getInternalPort())
            .user(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .maxSize(1)
            .build();
    try {
      sessionPool.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      try {
        sessionPool.executeNonQueryStatement(
            "CREATE TIMESERIES " + DATABASE + ".d1.s1 WITH DATATYPE=INT64, ENCODING=RLE");
        sessionPool.insertRecord(
            DATABASE + ".d1",
            1L,
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList(1L));
        sessionPool.insertRecord(
            DATABASE + ".d1",
            1L,
            Collections.singletonList("s1"),
            Collections.singletonList(TSDataType.INT64),
            Collections.singletonList(1L));

        try (SessionDataSetWrapper dataSet =
            sessionPool.executeQueryStatement("SELECT s1 FROM " + DATABASE + ".d1")) {
          assertTrue(dataSet.hasNext());
          assertEquals(1L, dataSet.next().getFields().get(0).getLongV());
        }
      } finally {
        sessionPool.executeNonQueryStatement("DELETE DATABASE " + DATABASE);
      }
    } finally {
      sessionPool.close();
    }
  }

  @Test
  public void testInternalSessionWithoutAutoFetchUsesConfiguredInternalEndpoint() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);

    try (Session session =
        new InternalSessionBuilder()
            .enableAutoFetch(false)
            .host(dataNode.getInternalAddress())
            .port(dataNode.getInternalPort())
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      session.open();
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      try {
        session.executeNonQueryStatement(
            "CREATE TIMESERIES " + DATABASE + ".d1.s1 WITH DATATYPE=INT64, ENCODING=RLE");
      } finally {
        session.executeNonQueryStatement("DELETE DATABASE " + DATABASE);
      }
    }
  }
}
