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

package org.apache.iotdb.subscription.it.dual;

import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.junit.After;
import org.junit.Before;

public abstract class AbstractSubscriptionDualIT extends AbstractSubscriptionIT {

  protected BaseEnv senderEnv;
  protected BaseEnv receiverEnv;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    setUpConfig();

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  protected void setUpConfig() {
    // enable auto create schema
    senderEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();

    super.tearDown();
  }
}
