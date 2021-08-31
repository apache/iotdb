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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class CommitLogTaskTest {

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void test() {
    RaftLogManager manager = new TestLogManager(0);
    try {
      manager.append(TestUtils.prepareTestLogs(10));

      AtomicBoolean complete = new AtomicBoolean(false);
      CommitLogTask task = new CommitLogTask(manager, 9, 9);
      AsyncMethodCallback<Void> callback =
          new AsyncMethodCallback<Void>() {
            @Override
            public void onComplete(Void unused) {
              complete.set(true);
            }

            @Override
            public void onError(Exception e) {
              fail(e.getMessage());
            }
          };
      task.registerCallback(callback);

      task.run();
      assertEquals(9, manager.getCommitLogIndex());
      assertTrue(complete.get());

      complete.set(false);
      task.run();
      assertFalse(complete.get());
    } finally {
      manager.close();
    }
  }
}
