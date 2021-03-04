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
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class LogCatchUpHandlerTest {

  private RaftMember member;

  @Before
  public void setUp() {
    member = new TestMetaGroupMember();
  }

  @After
  public void tearDown() throws IOException {
    member.closeLogManager();
    member.stop();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testComplete() throws InterruptedException {
    Node follower = TestUtils.getNode(1);
    Log log = new TestLog();
    AtomicBoolean appendSucceed = new AtomicBoolean();
    LogCatchUpHandler handler = new LogCatchUpHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setFollower(follower);
    handler.setLog(log);
    handler.setRaftMember(member);
    synchronized (appendSucceed) {
      new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      appendSucceed.wait();
    }
    assertTrue(appendSucceed.get());
  }

  @Test
  public void testLogMismatch() throws InterruptedException {
    Node follower = TestUtils.getNode(1);
    Log log = new TestLog();
    AtomicBoolean appendSucceed = new AtomicBoolean();
    LogCatchUpHandler handler = new LogCatchUpHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setFollower(follower);
    handler.setLog(log);
    handler.setRaftMember(member);
    synchronized (appendSucceed) {
      new Thread(() -> handler.onComplete(Response.RESPONSE_LOG_MISMATCH)).start();
      appendSucceed.wait();
    }
    assertTrue(appendSucceed.get());
  }

  @Test
  public void testLeadershipStale() throws InterruptedException {
    Node follower = TestUtils.getNode(1);
    Log log = new TestLog();
    AtomicBoolean appendSucceed = new AtomicBoolean();
    LogCatchUpHandler handler = new LogCatchUpHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setFollower(follower);
    handler.setLog(log);
    handler.setRaftMember(member);
    synchronized (appendSucceed) {
      new Thread(() -> handler.onComplete(100L)).start();
      appendSucceed.wait();
    }
    assertFalse(appendSucceed.get());
    assertEquals(100, member.getTerm().get());
  }

  @Test
  public void testError() throws InterruptedException {
    Node follower = TestUtils.getNode(1);
    Log log = new TestLog();
    AtomicBoolean appendSucceed = new AtomicBoolean();
    LogCatchUpHandler handler = new LogCatchUpHandler();
    handler.setAppendSucceed(appendSucceed);
    handler.setFollower(follower);
    handler.setLog(log);
    handler.setRaftMember(member);
    synchronized (appendSucceed) {
      new Thread(() -> handler.onError(new TestException())).start();
      appendSucceed.wait();
    }
    assertFalse(appendSucceed.get());
  }
}
