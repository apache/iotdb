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

package org.apache.iotdb.subscription.it.triple;

import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.session.subscription.consumer.SubscriptionExecutorServiceManager;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.junit.After;
import org.junit.Before;

public abstract class AbstractSubscriptionTripleIT extends AbstractSubscriptionIT {

  protected BaseEnv sender;
  protected BaseEnv receiver1;
  protected BaseEnv receiver2;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    // increase the number of threads to speed up testing
    SubscriptionExecutorServiceManager.setControlFlowExecutorCorePoolSize(4);
    SubscriptionExecutorServiceManager.setDownstreamDataFlowExecutorCorePoolSize(4);

    MultiEnvFactory.createEnv(3);
    sender = MultiEnvFactory.getEnv(0);
    receiver1 = MultiEnvFactory.getEnv(1);
    receiver2 = MultiEnvFactory.getEnv(2);

    setUpConfig();

    sender.initClusterEnvironment();
    receiver1.initClusterEnvironment();
    receiver2.initClusterEnvironment();
  }

  protected void setUpConfig() {
    // enable auto create schema
    sender.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    receiver1.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
    receiver2.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

    // 10 min, assert that the operations will not time out
    sender.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiver1.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
    receiver2.getConfig().getCommonConfig().setDnConnectionTimeoutMs(600000);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    sender.cleanClusterEnvironment();
    receiver1.cleanClusterEnvironment();
    receiver2.cleanClusterEnvironment();

    super.tearDown();
  }
}
