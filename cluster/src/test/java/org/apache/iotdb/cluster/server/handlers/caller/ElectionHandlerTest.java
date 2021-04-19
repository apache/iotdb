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
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ElectionHandlerTest {

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
  public void testAgreement() throws InterruptedException {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    AtomicInteger failingVoteCounter = new AtomicInteger(5);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (member.getTerm()) {
      for (int i = 0; i < 5; i++) {
        ElectionHandler handler =
            new ElectionHandler(
                member,
                TestUtils.getNode(i),
                electorTerm,
                quorum,
                terminated,
                electionValid,
                failingVoteCounter);
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      member.getTerm().wait();
    }
    assertEquals(0, quorum.get());
    assertTrue(electionValid.get());
    assertTrue(terminated.get());
  }

  @Test
  public void testLogMismatch() {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);
    for (int i = 0; i < 3; i++) {
      ElectionHandler handler =
          new ElectionHandler(
              member,
              TestUtils.getNode(i),
              electorTerm,
              quorum,
              terminated,
              electionValid,
              new AtomicInteger(5));
      handler.onComplete(Response.RESPONSE_AGREE);
    }
    for (int i = 6; i < 10; i++) {
      ElectionHandler handler =
          new ElectionHandler(
              member,
              TestUtils.getNode(i),
              electorTerm,
              quorum,
              terminated,
              electionValid,
              new AtomicInteger(5));
      handler.onComplete(electorTerm - 3);
    }
    assertFalse(electionValid.get());
  }

  @Test
  public void testTermTooSmall() throws InterruptedException {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);
    synchronized (member.getTerm()) {
      for (int i = 0; i < 3; i++) {
        ElectionHandler handler =
            new ElectionHandler(
                member,
                TestUtils.getNode(i),
                electorTerm,
                quorum,
                terminated,
                electionValid,
                new AtomicInteger(5));
        new Thread(() -> handler.onComplete(Response.RESPONSE_AGREE)).start();
      }
      for (int i = 3; i < 6; i++) {
        ElectionHandler handler =
            new ElectionHandler(
                member,
                TestUtils.getNode(i),
                electorTerm,
                quorum,
                terminated,
                electionValid,
                new AtomicInteger(5));
        new Thread(() -> handler.onComplete(electorTerm + 3)).start();
      }
      member.getTerm().wait();
    }
    assertFalse(electionValid.get());
    assertTrue(terminated.get());
    assertEquals(electorTerm + 3, member.getTerm().get());
  }

  @Test
  public void testError() {
    AtomicBoolean terminated = new AtomicBoolean(false);
    AtomicBoolean electionValid = new AtomicBoolean(false);
    long electorTerm = 10;
    AtomicInteger quorum = new AtomicInteger(5);

    ElectionHandler handler =
        new ElectionHandler(
            member,
            TestUtils.getNode(0),
            electorTerm,
            quorum,
            terminated,
            electionValid,
            new AtomicInteger(5));
    handler.onError(new TestException());

    assertEquals(5, quorum.get());
    assertFalse(electionValid.get());
    assertFalse(terminated.get());
  }
}
