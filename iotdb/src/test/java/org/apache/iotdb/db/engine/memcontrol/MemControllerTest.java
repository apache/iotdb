/**
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
package org.apache.iotdb.db.engine.memcontrol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.exception.MemControlException;
import org.junit.After;
import org.junit.Test;

public class MemControllerTest {

  private static long GB = 1024 * 1024 * 1024L;
  private static long MB = 1024 * 1024L;
  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @After
  public void tearDown() {
    BasicMemController.getInstance().clear();
  }

  @Test
  public void testNormal() throws MemControlException {
    BasicMemController memController = BasicMemController.getInstance();
    if (memController instanceof RecordMemController) {
      testRecordMemController();
    }
  }

  @Test
  public void testUnregisteredUser(){
    BasicMemController memController = BasicMemController.getInstance();
    DummyUser dummyUser = new DummyUser();
    boolean caught = false;
    try {
      memController.acquireUsage(dummyUser, 10);
    } catch (MemControlException e) {
      caught = true;
    }
    assertTrue(caught);

    caught = false;
    try {
      memController.releaseUsage(dummyUser, 10);
    } catch (MemControlException e) {
      caught = true;
    }
    assertTrue(caught);
  }

  private void testRecordMemController() throws MemControlException {
    BasicMemController memController = BasicMemController.getInstance();
    memController.clear();
    memController.setWarningThreshold(8 * GB);
    memController.setDangerousThreshold(16 * GB);

    DummyUser[] dummyUsers = new DummyUser[20];
    for (int i = 0; i < dummyUsers.length; i++) {
      dummyUsers[i] = new DummyUser();
      dummyUsers[i].register();
    }

    // every one request 1 GB, should get 7 safes, 8 warning and 5 dangerous
    for (int i = 0; i < 7; i++) {
      BasicMemController.UsageLevel level = memController.acquireUsage(dummyUsers[i], 1 * GB);
      assertEquals(BasicMemController.UsageLevel.SAFE, level);
    }
    for (int i = 7; i < 15; i++) {
      BasicMemController.UsageLevel level = memController.acquireUsage(dummyUsers[i], 1 * GB);
      assertEquals(BasicMemController.UsageLevel.WARNING, level);
    }
    for (int i = 15; i < 20; i++) {
      BasicMemController.UsageLevel level = memController.acquireUsage(dummyUsers[i], 1 * GB);
      assertEquals(BasicMemController.UsageLevel.DANGEROUS, level);
    }
    assertEquals(20 * GB, memController.getTotalUsage());
    // every one free its mem
    for (int i = 0; i < 7; i++) {
      memController.releaseUsage(dummyUsers[i], 1 * GB);
      assertEquals((19 - i) * GB, memController.getTotalUsage());
    }
    for (int i = 7; i < 20; i++) {
      memController.releaseUsage(dummyUsers[i], 2 * GB);
      assertEquals((19 - i) * GB, memController.getTotalUsage());
    }
    // ask for a too big mem
    BasicMemController.UsageLevel level = memController.acquireUsage(dummyUsers[0], 100 * GB);
    assertEquals(BasicMemController.UsageLevel.DANGEROUS, level);
    memController.releaseUsage(dummyUsers[0], 100 * GB);
    // single user ask continuously
    for (int i = 0; i < 8 * 1024 - 1; i++) {
      level = memController.acquireUsage(dummyUsers[0], 1 * MB);
      assertEquals(BasicMemController.UsageLevel.SAFE, level);
    }
    for (int i = 8 * 1024 - 1; i < 16 * 1024 - 1; i++) {
      level = memController.acquireUsage(dummyUsers[0], 1 * MB);
      assertEquals(BasicMemController.UsageLevel.WARNING, level);
    }
    for (int i = 16 * 1024 - 1; i < 17 * 1024; i++) {
      level = memController.acquireUsage(dummyUsers[0], 1 * MB);
      assertEquals(BasicMemController.UsageLevel.DANGEROUS, level);
    }

    for (int i = 0; i < dummyUsers.length; i++) {
      dummyUsers[i].unregister();
    }
  }

  class DummyUser implements MemUser {

  }
}
