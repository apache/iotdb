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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.junit.Before;
import org.junit.Test;

public class HeartbeatHandlerTest {

  private MetaGroupMember metaGroupMember;
  private boolean catchUpFlag;

  @Before
  public void setUp() {
    metaGroupMember = new TestMetaGroupMember() {
      @Override
      public void catchUp(Node follower, long followerLastLogIndex) {
        synchronized (metaGroupMember) {
          catchUpFlag = true;
          metaGroupMember.notifyAll();
        }
      }

      @Override
      public LogManager getLogManager() {
        return new TestLogManager();
      }
    };
  }

  @Test
  public void testComplete() throws InterruptedException {
    HeartbeatHandler handler = new HeartbeatHandler(metaGroupMember, TestUtils.getNode(1));
    HeartbeatResponse response = new HeartbeatResponse();
    response.setTerm(Response.RESPONSE_AGREE);
    response.setLastLogIndex(-1);
    catchUpFlag = false;
    synchronized (metaGroupMember) {
      new Thread(() -> handler.onComplete(response)).start();
      metaGroupMember.wait(10 * 1000);
    }
    assertTrue(catchUpFlag);
  }

  @Test
  public void testLeaderShipStale() throws InterruptedException {
    HeartbeatHandler handler = new HeartbeatHandler(metaGroupMember, TestUtils.getNode(1));
    HeartbeatResponse response = new HeartbeatResponse();
    response.setTerm(10);
    synchronized (metaGroupMember.getTerm()) {
      new Thread(() -> handler.onComplete(response)).start();
      metaGroupMember.getTerm().wait(1000);
    }
    assertEquals(10, metaGroupMember.getTerm().get());
  }

  @Test
  public void testError() throws InterruptedException {
    HeartbeatHandler handler = new HeartbeatHandler(metaGroupMember, TestUtils.getNode(1));
    catchUpFlag = false;
    new Thread(() -> handler.onError(new TestException())).start();
    Thread.sleep(1000);
    assertFalse(catchUpFlag);
  }
}