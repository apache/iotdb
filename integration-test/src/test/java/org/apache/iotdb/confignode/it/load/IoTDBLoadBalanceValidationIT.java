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

package org.apache.iotdb.confignode.it.load;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.assertNonQueryTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTableNonQueryTestFail;

@RunWith(IoTDBTestRunner.class)
public class IoTDBLoadBalanceValidationIT {

  private static final String LOAD_BALANCE_TO_NONEXISTENT_DATANODE =
      "LOAD BALANCE TO DATANODE 10000";
  private static final String NONEXISTENT_DATANODE_ERROR =
      "Load balance target DataNodes [10000] do not exist or are not running.";

  @Before
  public void setUp() {
    EnvFactory.getEnv().initClusterEnvironment(1, 1);
  }

  @After
  public void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  @Category({LocalStandaloneIT.class, ClusterIT.class})
  public void testTreeLoadBalanceToNonexistentDataNode() {
    assertNonQueryTestFail(LOAD_BALANCE_TO_NONEXISTENT_DATANODE, NONEXISTENT_DATANODE_ERROR);
  }

  @Test
  @Category({TableLocalStandaloneIT.class, TableClusterIT.class})
  public void testTableLoadBalanceToNonexistentDataNode() {
    assertTableNonQueryTestFail(
        LOAD_BALANCE_TO_NONEXISTENT_DATANODE, NONEXISTENT_DATANODE_ERROR, null);
  }
}
