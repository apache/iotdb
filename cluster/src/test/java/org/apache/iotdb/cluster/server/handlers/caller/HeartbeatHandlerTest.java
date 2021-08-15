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
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.Constants;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class HeartbeatHandlerTest {

  private MetaGroupMember metaGroupMember;
  private boolean catchUpFlag;
  private long looseInconsistentNum = 5;

  @Before
  public void setUp() {
    metaGroupMember =
        new TestMetaGroupMember() {
          @Override
          public void catchUp(Node follower, long lastLogIdx) {
            synchronized (metaGroupMember) {
              catchUpFlag = true;
              metaGroupMember.notifyAll();
            }
          }
        };
    metaGroupMember.initPeerMap();
    metaGroupMember.setLogManager(new TestLogManager(1));
  }

  @After
  public void tearDown() throws IOException {
    metaGroupMember.closeLogManager();
    metaGroupMember.stop();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testComplete() {
    HeartbeatHandler handler = new HeartbeatHandler(metaGroupMember, TestUtils.getNode(1));
    HeartBeatResponse response = new HeartBeatResponse();
    response.setTerm(Response.RESPONSE_AGREE);
    response.setLastLogTerm(-2);
    response.setFollower(
        new Node("192.168.0.6", 9003, 6, 40010, Constants.RPC_PORT, "192.168.0.6"));
    catchUpFlag = false;
    for (int i = 0; i < looseInconsistentNum; i++) {
      handler.onComplete(response);
    }
    assertTrue(catchUpFlag);
  }

  @Test
  public void testLeaderShipStale() {
    HeartbeatHandler handler = new HeartbeatHandler(metaGroupMember, TestUtils.getNode(1));
    HeartBeatResponse response = new HeartBeatResponse();
    response.setTerm(10);
    handler.onComplete(response);
    assertEquals(10, metaGroupMember.getTerm().get());
  }

  @Test
  public void testError() {
    HeartbeatHandler handler = new HeartbeatHandler(metaGroupMember, TestUtils.getNode(1));
    catchUpFlag = false;
    handler.onError(new TestException());
    assertFalse(catchUpFlag);
  }
}
