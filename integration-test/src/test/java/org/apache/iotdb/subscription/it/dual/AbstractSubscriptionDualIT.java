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

import org.junit.After;
import org.junit.Before;

abstract class AbstractSubscriptionDualIT {

  protected BaseEnv senderEnv;
  protected BaseEnv receiverEnv;

  @Before
  public void setUp() {
    MultiEnvFactory.createEnv(2);
    senderEnv = MultiEnvFactory.getEnv(0);
    receiverEnv = MultiEnvFactory.getEnv(1);

    // enable auto create schema
    senderEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

    // for IoTDBSubscriptionConsumerGroupIT
    receiverEnv.getConfig().getCommonConfig().setPipeAirGapReceiverEnabled(true);

    // 10 min, assert that the operations will not time out
    senderEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);
    receiverEnv.getConfig().getCommonConfig().setCnConnectionTimeoutMs(600000);

    senderEnv.initClusterEnvironment();
    receiverEnv.initClusterEnvironment();
  }

  @After
  public final void tearDown() {
    senderEnv.cleanClusterEnvironment();
    receiverEnv.cleanClusterEnvironment();
  }
}
