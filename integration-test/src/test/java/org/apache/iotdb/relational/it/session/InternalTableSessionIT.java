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

import org.apache.iotdb.commons.client.internal.session.InternalTableSessionBuilder;
import org.apache.iotdb.commons.client.internal.session.InternalTableSessionPoolBuilder;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.pool.ITableSessionPool;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.InternalSessionITUtils;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class InternalTableSessionIT {

  private static final String DATABASE = "internal_table_session_it";
  private static final String TABLE = "t";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInternalTableSessionUsesInternalPort() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (ITableSession session =
        new InternalTableSessionBuilder(InternalSessionITUtils.createInternalNodeSupplier())
            .nodeUrls(
                Collections.singletonList(
                    InternalSessionITUtils.getEndPointUrl(
                        InternalSessionITUtils.getInternalEndPoint(dataNode))))
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      try {
        createTableAndInsert(session);
        assertTableValue(session);
      } finally {
        session.executeNonQueryStatement("DROP DATABASE " + DATABASE);
      }
    }
  }

  @Test
  public void testInternalTableSessionPoolUsesInternalPort() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (ITableSessionPool sessionPool =
        new InternalTableSessionPoolBuilder(InternalSessionITUtils.createInternalNodeSupplier())
            .nodeUrls(
                Collections.singletonList(
                    InternalSessionITUtils.getEndPointUrl(
                        InternalSessionITUtils.getInternalEndPoint(dataNode))))
            .user(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .maxSize(1)
            .build()) {
      try (ITableSession session = sessionPool.getSession()) {
        session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
        try {
          createTableAndInsert(session);
          assertTableValue(session);
        } finally {
          session.executeNonQueryStatement("DROP DATABASE " + DATABASE);
        }
      }
    }
  }

  @Test
  public void testInternalTableSessionWithoutAutoFetchUsesConfiguredInternalEndpoint()
      throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    try (ITableSession session =
        new InternalTableSessionBuilder()
            .enableAutoFetch(false)
            .nodeUrls(
                Collections.singletonList(
                    InternalSessionITUtils.getEndPointUrl(
                        InternalSessionITUtils.getInternalEndPoint(dataNode))))
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      session.executeNonQueryStatement("CREATE DATABASE " + DATABASE);
      try {
        session.executeNonQueryStatement(
            "CREATE TABLE " + DATABASE + "." + TABLE + " (tag1 STRING TAG, value INT64 FIELD)");
      } finally {
        session.executeNonQueryStatement("DROP DATABASE " + DATABASE);
      }
    }
  }

  private static void createTableAndInsert(final ITableSession session) throws Exception {
    session.executeNonQueryStatement("USE " + DATABASE);
    session.executeNonQueryStatement(
        "CREATE TABLE " + TABLE + " (tag1 STRING TAG, value INT64 FIELD)");
    session.insert(createTablet());
    session.insert(createTablet());
  }

  private static void assertTableValue(final ITableSession session) throws Exception {
    try (SessionDataSet dataSet =
        session.executeQueryStatement("SELECT value FROM " + DATABASE + "." + TABLE)) {
      assertTrue(dataSet.hasNext());
      assertEquals(42L, dataSet.next().getFields().get(0).getLongV());
    }
  }

  private static Tablet createTablet() {
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
    tablet.addTimestamp(0, 1L);
    tablet.addValue("tag1", 0, "d1");
    tablet.addValue("value", 0, 42L);
    return tablet;
  }
}
