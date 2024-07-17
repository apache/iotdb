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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.pipe.datastructure.queue.ConcurrentIterableLinkedQueue;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeEnrichedPlan;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.pipe.api.event.Event;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import static org.apache.iotdb.db.utils.constant.TestConstant.BASE_OUTPUT_PATH;

public class ConfigRegionListeningQueueTest {
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "snapshot");

  @BeforeClass
  public static void setup() {
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
    PipeConfigNodeAgent.runtime().listener().open();
    PipeConfigNodeAgent.runtime().notifyLeaderReady();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    PipeConfigNodeAgent.runtime().listener().close();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void testSnapshot() throws TException, IOException, AuthException {
    final DatabaseSchemaPlan plan1 =
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.test1"));
    final PipeEnrichedPlan plan2 =
        new PipeEnrichedPlan(
            new AuthorPlan(
                ConfigPhysicalPlanType.CreateUser,
                "user0",
                "",
                "passwd",
                "",
                new HashSet<>(),
                false,
                new ArrayList<>()));

    PipeConfigNodeAgent.runtime().listener().tryListenToPlan(plan1, false);
    PipeConfigNodeAgent.runtime().listener().tryListenToPlan(plan2, false);

    // tryListenToSnapshots() cannot be tested here since we cannot operate the reference count of
    // the original or deserialized events

    PipeConfigNodeAgent.runtime().listener().processTakeSnapshot(snapshotDir);
    PipeConfigNodeAgent.runtime().listener().close();

    PipeConfigNodeAgent.runtime().listener().processLoadSnapshot(snapshotDir);
    Assert.assertTrue(PipeConfigNodeAgent.runtime().listener().isOpened());
    ConcurrentIterableLinkedQueue<Event>.DynamicIterator itr =
        PipeConfigNodeAgent.runtime().listener().newIterator(0);

    final Event event1 = itr.next(0);
    Assert.assertEquals(plan1, ((PipeConfigRegionWritePlanEvent) event1).getConfigPhysicalPlan());

    final Event event2 = itr.next(0);
    Assert.assertEquals(
        plan2.getInnerPlan(), ((PipeConfigRegionWritePlanEvent) event2).getConfigPhysicalPlan());
    Assert.assertTrue(((PipeConfigRegionWritePlanEvent) event2).isGeneratedByPipe());

    Assert.assertNull(itr.next(0));
  }
}
