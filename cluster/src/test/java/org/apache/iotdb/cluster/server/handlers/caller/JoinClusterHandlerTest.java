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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.Response;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JoinClusterHandlerTest {

  @Test
  public void testComplete() throws InterruptedException {
    Node contact = TestUtils.getNode(0);
    AtomicReference<AddNodeResponse> result = new AtomicReference<>();
    AddNodeResponse response = new AddNodeResponse();
    response.setRespNum((int) Response.RESPONSE_AGREE);
    response.setPartitionTableBytes(new byte[4096]);
    JoinClusterHandler handler = new JoinClusterHandler();
    handler.setContact(contact);
    handler.setResponse(result);
    synchronized (result) {
      new Thread(() -> handler.onComplete(response)).start();
      result.wait();
    }
    assertEquals(response, result.get());
  }

  @Test
  public void testError() throws InterruptedException {
    Node contact = TestUtils.getNode(0);
    AtomicReference<AddNodeResponse> result = new AtomicReference<>();
    JoinClusterHandler handler = new JoinClusterHandler();
    handler.setContact(contact);
    handler.setResponse(result);
    synchronized (result) {
      new Thread(() -> handler.onError(new TestException())).start();
      result.wait();
    }
    assertNull(result.get());
  }
}
