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

package org.apache.iotdb.subscription.it.triple.regression.param;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegressionMisc;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegressionMisc.class})
public class IoTDBTestParamSubscriptionSessionIT extends AbstractSubscriptionRegressionIT {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Test
  public void testCreateSession_null_host() {
    new SubscriptionSession.Builder().host(null).build();
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testCreateSession_error_port() throws IoTDBConnectionException {
    new SubscriptionSession(SRC_HOST, SRC_PORT + 1).open();
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testCreateSession_ErrorHostname() throws IoTDBConnectionException {
    new SubscriptionSession.Builder().host("noName").build().open();
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testCreateSession_ErrorUsername() throws IoTDBConnectionException {
    new SubscriptionSession.Builder()
        .host(SRC_HOST)
        .port(SRC_PORT)
        .username("admin")
        .build()
        .open();
  }

  @Test(expected = IoTDBConnectionException.class)
  public void testCreateSession_ErrorPassword() throws IoTDBConnectionException {
    new SubscriptionSession.Builder()
        .host(SRC_HOST)
        .port(SRC_PORT)
        .password("admin")
        .build()
        .open();
  }
}
