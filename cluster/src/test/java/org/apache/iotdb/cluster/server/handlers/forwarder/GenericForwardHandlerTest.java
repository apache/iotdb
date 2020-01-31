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

package org.apache.iotdb.cluster.server.handlers.forwarder;

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Test;

public class GenericForwardHandlerTest {

  @Test
  public void testComplete() {
    AtomicBoolean flag = new AtomicBoolean(false);
    GenericForwardHandler handler = new GenericForwardHandler(new AsyncMethodCallback() {
      @Override
      public void onComplete(Object o) {
        flag.set(true);
      }

      @Override
      public void onError(Exception e) {

      }
    });

    handler.onComplete(new AddNodeResponse());
    assertTrue(flag.get());
  }

  @Test
  public void testError() {
    AtomicBoolean flag = new AtomicBoolean(false);
    GenericForwardHandler handler = new GenericForwardHandler(new AsyncMethodCallback() {
      @Override
      public void onComplete(Object o) {

      }

      @Override
      public void onError(Exception e) {
        flag.set(true);
      }
    });

    handler.onError(new TestException());
    assertTrue(flag.get());
  }
}