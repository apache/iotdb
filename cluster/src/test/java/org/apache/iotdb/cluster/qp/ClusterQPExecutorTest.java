/**
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
package org.apache.iotdb.cluster.qp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.qp.executor.NonQueryExecutor;
import org.apache.iotdb.cluster.rpc.service.TSServiceClusterImpl;
import org.apache.iotdb.cluster.utils.EnvironmentUtils;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClusterQPExecutorTest {

  private static final ClusterConfig CLUSTER_CONFIG = ClusterDescriptor.getInstance().getConfig();

  private TSServiceClusterImpl impl;

  private NonQueryExecutor executor;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    impl = new TSServiceClusterImpl();
    impl.initClusterService();

    executor = impl.getNonQueryExecutor();
  }

  @After
  public void tearDown() throws Exception {
    impl.closeClusterService();
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void setReadMetadataConsistencyLevel() throws Exception {
    assertEquals(CLUSTER_CONFIG.getReadMetadataConsistencyLevel(),
        executor.getReadMetadataConsistencyLevel());
    boolean exec;
    exec= impl.execSetConsistencyLevel("set read metadata level 1");
    assertTrue(exec);
    assertEquals(1, executor.getReadMetadataConsistencyLevel());

    exec= impl.execSetConsistencyLevel("show timeseries");
    assertEquals(1, executor.getReadMetadataConsistencyLevel());
    assertFalse(exec);

    exec= impl.execSetConsistencyLevel("set read metadata level 2");
    assertTrue(exec);
    assertEquals(2, executor.getReadMetadataConsistencyLevel());

    exec = impl.execSetConsistencyLevel("set read metadata level -2");
    assertEquals(2, executor.getReadMetadataConsistencyLevel());
    assertFalse(exec);

    try {
      impl.execSetConsistencyLevel("set read metadata level 90");
    } catch (Exception e) {
      assertEquals("Consistency level 90 not support", e.getMessage());
    }
    assertEquals(2, executor.getReadMetadataConsistencyLevel());
  }

  @Test
  public void setReadDataConsistencyLevel() throws Exception {
    assertEquals(CLUSTER_CONFIG.getReadDataConsistencyLevel(),
        executor.getReadDataConsistencyLevel());
    boolean exec;
    exec= impl.execSetConsistencyLevel("set read data level 1");
    assertTrue(exec);
    assertEquals(1, executor.getReadDataConsistencyLevel());

    exec= impl.execSetConsistencyLevel("show timeseries");
    assertEquals(1, executor.getReadDataConsistencyLevel());
    assertFalse(exec);

    exec= impl.execSetConsistencyLevel("set read data level 2");
    assertTrue(exec);
    assertEquals(2, executor.getReadDataConsistencyLevel());

    exec = impl.execSetConsistencyLevel("set read data level -2");
    assertEquals(2, executor.getReadDataConsistencyLevel());
    assertFalse(exec);

    try {
      impl.execSetConsistencyLevel("set read data level 90");
    } catch (Exception e) {
      assertEquals("Consistency level 90 not support", e.getMessage());
    }
    assertEquals(2, executor.getReadDataConsistencyLevel());
  }
}