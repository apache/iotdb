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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTsFileQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;

import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

public class SubscriptionPipeTsFileEventBatchCleanupTest {

  @Test
  public void testDeletesSealedFilesAfterAllSharedEventsAreCleaned() throws Exception {
    final File temporaryDirectory =
        Files.createTempDirectory("subscription-tsfile-cleanup").toFile();
    final File firstFile = new File(temporaryDirectory, "first.tsfile");
    final File secondFile = new File(temporaryDirectory, "second.tsfile");
    Assert.assertTrue(firstFile.createNewFile());
    Assert.assertTrue(secondFile.createNewFile());

    final PipeTabletEventTsFileBatch innerBatch = Mockito.mock(PipeTabletEventTsFileBatch.class);
    final SubscriptionPrefetchingTsFileQueue queue =
        Mockito.mock(SubscriptionPrefetchingTsFileQueue.class);
    Mockito.when(innerBatch.isEmpty()).thenReturn(false);
    Mockito.when(innerBatch.sealTsFiles())
        .thenReturn(Arrays.asList(new Pair<>("db1", firstFile), new Pair<>("db2", secondFile)));
    Mockito.when(queue.generateSubscriptionCommitContext())
        .thenReturn(
            new SubscriptionCommitContext(1, 1, "topic", "group", 1),
            new SubscriptionCommitContext(1, 1, "topic", "group", 2));

    try {
      final SubscriptionPipeTsFileEventBatch batch =
          new SubscriptionPipeTsFileEventBatch(1, queue, 1, 1, innerBatch);
      final List<SubscriptionEvent> events = batch.generateSubscriptionEvents();

      Assert.assertEquals(2, events.size());
      events.get(0).cleanUp(false);
      Assert.assertTrue(firstFile.exists());
      Assert.assertTrue(secondFile.exists());

      events.get(1).cleanUp(false);
      Assert.assertFalse(firstFile.exists());
      Assert.assertFalse(secondFile.exists());
      Mockito.verify(innerBatch, Mockito.times(1)).close();
    } finally {
      FileUtils.deleteDirectory(temporaryDirectory);
    }
  }
}
