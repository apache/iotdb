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

package org.apache.iotdb.subscription.it;

import org.apache.iotdb.session.subscription.consumer.SubscriptionExecutorServiceManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;

public abstract class AbstractSubscriptionIT {

  @Rule public DisplayName testName = new DisplayName();

  @Rule
  public final TestRule skipOnSetUpAndTearDownFailure =
      new SkipOnSetUpAndTearDownFailure("setUp", "tearDown");

  @Before
  public void setUp() throws Exception {
    // set thread name
    Thread.currentThread().setName(String.format("%s - main", testName.getDisplayName()));

    // set thread pools core size
    SubscriptionExecutorServiceManager.setControlFlowExecutorCorePoolSize(1);
    SubscriptionExecutorServiceManager.setUpstreamDataFlowExecutorCorePoolSize(1);
    SubscriptionExecutorServiceManager.setDownstreamDataFlowExecutorCorePoolSize(1);
  }

  @After
  public void tearDown() throws Exception {}
}
