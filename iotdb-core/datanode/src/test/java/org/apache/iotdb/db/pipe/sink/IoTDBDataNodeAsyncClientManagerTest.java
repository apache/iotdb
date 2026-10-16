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

package org.apache.iotdb.db.pipe.sink;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.pipe.sink.client.IoTDBDataNodeAsyncClientManager;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

public class IoTDBDataNodeAsyncClientManagerTest {

  @Test
  public void testClientResourcesShouldDifferentiateEndPoints() throws Exception {
    final IoTDBDataNodeAsyncClientManager firstManager =
        new IoTDBDataNodeAsyncClientManager(
            Collections.singletonList(new TEndPoint("127.0.0.1", 6667)),
            false,
            "round-robin",
            "user",
            "password",
            true,
            "sync",
            true,
            true,
            true);
    final IoTDBDataNodeAsyncClientManager secondManager =
        new IoTDBDataNodeAsyncClientManager(
            Collections.singletonList(new TEndPoint("127.0.0.2", 6667)),
            false,
            "round-robin",
            "user",
            "password",
            true,
            "sync",
            true,
            true,
            true);

    try {
      Assert.assertEquals(
          getReceiverAttributes(firstManager), getReceiverAttributes(secondManager));
      Assert.assertNotEquals(
          getClientResourceKey(firstManager), getClientResourceKey(secondManager));
      Assert.assertNotSame(getEndPoint2Client(firstManager), getEndPoint2Client(secondManager));
      Assert.assertNotSame(getExecutor(firstManager), getExecutor(secondManager));
    } finally {
      firstManager.close();
      secondManager.close();
    }
  }

  private static String getReceiverAttributes(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field =
        IoTDBDataNodeAsyncClientManager.class.getDeclaredField("receiverAttributes");
    field.setAccessible(true);
    return (String) field.get(manager);
  }

  private static String getClientResourceKey(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field = IoTDBDataNodeAsyncClientManager.class.getDeclaredField("clientResourceKey");
    field.setAccessible(true);
    return (String) field.get(manager);
  }

  private static Object getEndPoint2Client(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field = IoTDBDataNodeAsyncClientManager.class.getDeclaredField("endPoint2Client");
    field.setAccessible(true);
    return field.get(manager);
  }

  private static ExecutorService getExecutor(final IoTDBDataNodeAsyncClientManager manager)
      throws Exception {
    final Field field = IoTDBDataNodeAsyncClientManager.class.getDeclaredField("executor");
    field.setAccessible(true);
    return (ExecutorService) field.get(manager);
  }
}
