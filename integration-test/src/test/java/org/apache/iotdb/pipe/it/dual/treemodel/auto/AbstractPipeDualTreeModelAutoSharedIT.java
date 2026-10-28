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

package org.apache.iotdb.pipe.it.dual.treemodel.auto;

import org.apache.iotdb.db.it.utils.TestUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/** Shares the default dual-cluster configuration within one test class. */
public abstract class AbstractPipeDualTreeModelAutoSharedIT
    extends AbstractPipeDualTreeModelAutoIT {

  private static final AbstractPipeDualTreeModelAutoIT CLUSTER =
      new AbstractPipeDualTreeModelAutoIT() {};

  @BeforeClass
  public static void setUpCluster() {
    CLUSTER.setUp();
  }

  @AfterClass
  public static void tearDownCluster() {
    try {
      if (CLUSTER.senderEnv != null) {
        CLUSTER.senderEnv.cleanClusterEnvironment();
      }
    } finally {
      if (CLUSTER.receiverEnv != null) {
        CLUSTER.receiverEnv.cleanClusterEnvironment();
      }
    }
  }

  @Override
  @Before
  public void setUp() {
    senderEnv = CLUSTER.senderEnv;
    receiverEnv = CLUSTER.receiverEnv;
  }

  @Override
  @After
  public final void tearDown() {
    try {
      TestUtils.dropAllPipes(senderEnv);
      TestUtils.dropAllPipes(receiverEnv);
      TestUtils.clearAllDatabases(senderEnv);
      TestUtils.clearAllDatabases(receiverEnv);
    } catch (final Exception e) {
      throw new RuntimeException("Failed to clean the shared Pipe test clusters", e);
    }
  }
}
