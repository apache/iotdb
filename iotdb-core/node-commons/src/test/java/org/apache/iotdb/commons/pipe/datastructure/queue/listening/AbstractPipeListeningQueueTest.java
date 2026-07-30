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

package org.apache.iotdb.commons.pipe.datastructure.queue.listening;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public class AbstractPipeListeningQueueTest {

  @Test
  public void testDeserializeReleasesExistingQueueAndSnapshotCache() throws Exception {
    final Path temporaryDirectory = Files.createTempDirectory("pipe-listening-queue-snapshot");
    final File snapshotFile = temporaryDirectory.resolve("queue.snapshot").toFile();
    final TestPipeListeningQueue emptyQueue = new TestPipeListeningQueue();
    final TestPipeListeningQueue queue = new TestPipeListeningQueue();
    final EnrichedEvent queuedEvent = Mockito.mock(EnrichedEvent.class);
    final PipeSnapshotEvent cachedSnapshot = Mockito.mock(PipeSnapshotEvent.class);
    try {
      Assert.assertTrue(emptyQueue.serializeToFile(snapshotFile));

      queue.open();
      queue.addEvent(queuedEvent);
      queue.addSnapshots(Collections.singletonList(cachedSnapshot));
      Assert.assertEquals(1, queue.getSize());
      Assert.assertEquals(1, queue.findAvailableSnapshots(false).getRight().size());

      queue.deserializeFromFile(snapshotFile);

      Mockito.verify(queuedEvent)
          .decreaseReferenceCount(AbstractPipeListeningQueue.class.getName(), false);
      Mockito.verify(cachedSnapshot)
          .decreaseReferenceCount(AbstractPipeListeningQueue.class.getName(), false);
      Assert.assertEquals(0, queue.getSize());
      Assert.assertTrue(queue.findAvailableSnapshots(false).getRight().isEmpty());
      Assert.assertFalse(queue.isOpened());
    } finally {
      Files.deleteIfExists(snapshotFile.toPath());
      Files.deleteIfExists(temporaryDirectory);
    }
  }

  private static final class TestPipeListeningQueue extends AbstractPipeListeningQueue {

    private void addEvent(final EnrichedEvent event) {
      tryListen(event);
    }

    private void addSnapshots(final List<PipeSnapshotEvent> snapshots) {
      tryListen(snapshots);
    }

    @Override
    protected ByteBuffer serializeToByteBuffer(final Event event) {
      return ByteBuffer.allocate(0);
    }

    @Override
    protected Event deserializeFromByteBuffer(final ByteBuffer byteBuffer) {
      return null;
    }
  }
}
