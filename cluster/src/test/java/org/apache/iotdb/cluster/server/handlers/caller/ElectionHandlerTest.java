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

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.cluster.common.TestException;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.junit.Before;
import org.junit.Test;

public class ElectionHandlerTest {

  private RaftMember member;

  @Before
  public void setUp() {
    member = new TestMetaGroupMember();
  }

  @Test
  public void testAgreement() throws InterruptedException {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (member.getTerm()) {
      for (int i = 0; i < 5; i++) {
        ElectionHandler handler = new ElectionHandler(member, TestUtils.getNode(i), electorTerm,
            quorum, terminated, electionValid);
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      member.getTerm().wait(10 * 1000);
    }
    assertEquals(0, quorum.get());
    assertTrue(electionValid.get());
    assertTrue(terminated.get());
  }

  @Test
  public void testLogMismatch() throws InterruptedException {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (member.getTerm()) {
      for (int i = 0; i < 3; i++) {
        ElectionHandler handler = new ElectionHandler(member, TestUtils.getNode(i), electorTerm,
            quorum, terminated, electionValid);
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      for (int i = 6; i < 10; i++) {
        ElectionHandler handler = new ElectionHandler(member, TestUtils.getNode(i), electorTerm,
            quorum, terminated, electionValid);
        new Thread(() -> handler.onComplete(electorTerm - 3)).start();
      }
      member.getTerm().wait(10 * 1000);
    }
    assertFalse(electionValid.get());
    assertTrue(terminated.get());
  }

  @Test
  public void testTermTooSmall() throws InterruptedException {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (member.getTerm()) {
      for (int i = 0; i < 3; i++) {
        ElectionHandler handler = new ElectionHandler(member, TestUtils.getNode(i), electorTerm,
            quorum, terminated, electionValid);
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      for (int i = 3; i < 6; i++) {
        ElectionHandler handler = new ElectionHandler(member, TestUtils.getNode(i), electorTerm,
            quorum, terminated, electionValid);
        new Thread(() -> handler.onComplete(electorTerm + 3)).start();
      }
      member.getTerm().wait(10 * 1000);
    }
    assertFalse(electionValid.get());
    assertTrue(terminated.get());
    assertEquals(electorTerm + 3, member.getTerm().get());
  }

  @Test
  public void testError() throws InterruptedException {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (member.getTerm()) {
      ElectionHandler handler = new ElectionHandler(member, TestUtils.getNode(0), electorTerm,
          quorum, terminated, electionValid);
      new Thread(() -> handler.onError(new TestException())).start();
      member.getTerm().wait(10 * 1000);
    }
    assertEquals(5, quorum.get());
    assertFalse(electionValid.get());
    assertFalse(terminated.get());
  }

}