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
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.DummyNodesSupplier;
import org.apache.iotdb.session.Session;

import org.apache.tsfile.enums.TSDataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class InternalSessionIT {

  private static final String DATABASE = "root.internal_session_it";

  @Before
  public void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testInternalSessionUsesInternalPort() throws Exception {
    final DataNodeWrapper dataNode = EnvFactory.getEnv().getDataNodeWrapper(0);
    final INodeSupplier nodeSupplier =
        new DummyNodesSupplier(
            Collections.singletonList(
                new TEndPoint(dataNode.getInternalAddress(), dataNode.getInternalPort())));

    try (Session session =
        new Session.Builder()
            .host(dataNode.getInternalAddress())
            .port(dataNode.getInternalPort())
            .username("root")
            .buildInternal(nodeSupplier)) {
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
}
