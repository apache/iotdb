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

package org.apache.iotdb.relational.it.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.internal.session.InternalTableSessionBuilder;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.InternalSessionITUtils;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class InternalTableSessionRedirectIT {

  private static final String DATABASE = "internal_table_session_redirect_it";
  private static final String TABLE = "t";

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
  public void testTabletInsertRedirectsToInternalLeaderEndpoint() throws Exception {
    final INodeSupplier nodeSupplier = InternalSessionITUtils.createInternalNodeSupplier();
    final TEndPoint initialEndPoint = InternalSessionITUtils.getInternalEndPoints().get(0);
    final Set<TEndPoint> leaderEndPoints;

    try (ITableSession setupSession =
        new InternalTableSessionBuilder(nodeSupplier)
            .nodeUrls(
                Collections.singletonList(InternalSessionITUtils.getEndPointUrl(initialEndPoint)))
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      setupSession.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      setupSession.executeNonQueryStatement(
          "CREATE TABLE " + DATABASE + "." + TABLE + " (tag1 STRING TAG, value INT64 FIELD)");
      setupSession.executeNonQueryStatement("USE " + DATABASE);
      // DataRegionGroups are allocated on the first data write, not when the database is created.
      setupSession.insert(createTablet(1L));
      leaderEndPoints =
          InternalSessionITUtils.awaitDataRegionLeaderInternalEndPoints(
              InternalSessionITUtils.getUnderlyingSession(setupSession), DATABASE);
    }
    final TEndPoint nonLeaderEndPoint =
        InternalSessionITUtils.findNonLeaderInternalEndPoint(leaderEndPoints);

    try (ITableSession session =
        new InternalTableSessionBuilder(nodeSupplier)
            .nodeUrls(
                Collections.singletonList(InternalSessionITUtils.getEndPointUrl(nonLeaderEndPoint)))
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .database(DATABASE)
            .build()) {
      session.insert(createTablet(2L));

      final Session underlyingSession = InternalSessionITUtils.getUnderlyingSession(session);
      final Map<?, TEndPoint> tableModelDeviceIdToEndpoint =
          InternalSessionITUtils.getSessionField(underlyingSession, "tableModelDeviceIdToEndpoint");
      assertEquals(1, tableModelDeviceIdToEndpoint.size());
      assertTrue(leaderEndPoints.contains(tableModelDeviceIdToEndpoint.values().iterator().next()));
    }
  }

  private static Tablet createTablet(final long timestamp) {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("value", TSDataType.INT64));
    final Tablet tablet =
        new Tablet(
            TABLE,
            IMeasurementSchema.getMeasurementNameList(schemas),
            IMeasurementSchema.getDataTypeList(schemas),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, timestamp);
    tablet.addValue("tag1", 0, "d1");
    tablet.addValue("value", 0, 42L);
    return tablet;
  }
}
