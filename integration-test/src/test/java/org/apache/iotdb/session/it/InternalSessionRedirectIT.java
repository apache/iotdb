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
import org.apache.iotdb.commons.client.internal.session.InternalSessionBuilder;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.InternalSessionITUtils;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(ClusterIT.class)
public class InternalSessionRedirectIT {

  private static final String DATABASE = "root.internal_session_redirect_it";
  private static final String DEVICE = DATABASE + ".d1";
  private static final String TIMESERIES = DEVICE + ".s1";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(1)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testLastDataQueryRedirectsToInternalLeaderEndpoint() throws Exception {
    final INodeSupplier nodeSupplier = InternalSessionITUtils.createInternalNodeSupplier();
    final TEndPoint initialEndPoint = InternalSessionITUtils.getInternalEndPoints().get(0);

    try (Session setupSession =
        new InternalSessionBuilder(nodeSupplier)
            .host(initialEndPoint.getIp())
            .port(initialEndPoint.getPort())
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      setupSession.open();
      setupSession.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      setupSession.executeNonQueryStatement(
          "CREATE TIMESERIES " + TIMESERIES + " WITH DATATYPE=INT64, ENCODING=RLE");
      setupSession.insertRecord(
          DEVICE,
          1L,
          Collections.singletonList("s1"),
          Collections.singletonList(TSDataType.INT64),
          Collections.singletonList(42L));
    }

    final Set<TEndPoint> leaderEndPoints;
    try (Session discoverySession =
        new InternalSessionBuilder(nodeSupplier)
            .host(initialEndPoint.getIp())
            .port(initialEndPoint.getPort())
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      discoverySession.open();
      leaderEndPoints =
          InternalSessionITUtils.awaitDataRegionLeaderInternalEndPoints(discoverySession, DATABASE);
    }
    final TEndPoint nonLeaderEndPoint =
        InternalSessionITUtils.findNonLeaderInternalEndPoint(leaderEndPoints);

    try (Session session =
        new InternalSessionBuilder(nodeSupplier)
            .host(nonLeaderEndPoint.getIp())
            .port(nonLeaderEndPoint.getPort())
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      session.open();
      session.insertRecord(
          DEVICE,
          2L,
          Collections.singletonList("s1"),
          Collections.singletonList(TSDataType.INT64),
          Collections.singletonList(43L));
      try (SessionDataSet dataSet =
          session.executeLastDataQuery(Collections.singletonList(TIMESERIES))) {
        assertTrue(dataSet.hasNext());
        dataSet.next();
      }

      final Map<String, TEndPoint> deviceIdToEndpoint =
          InternalSessionITUtils.getSessionField(session, "deviceIdToEndpoint");
      assertEquals(1, deviceIdToEndpoint.size());
      assertTrue(leaderEndPoints.contains(deviceIdToEndpoint.get(DEVICE)));
    }
  }
}
