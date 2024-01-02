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

package org.apache.iotdb.commons.pipe.datastructure;

import org.apache.iotdb.commons.pipe.event.PipeSnapshotEvent;
import org.apache.iotdb.commons.pipe.task.PipeTask;
import org.apache.iotdb.pipe.api.event.Event;

import java.io.IOException;
import java.util.Objects;

/**
 * {@link AbstractPipeListeningQueue} is the encapsulation of the {@link
 * AbstractSerializableListeningQueue} to enable using reference count to control opening and
 * closing. This class also enables a {@link PipeTask} to find the snapshots close enough to send if
 * existed.
 */
public abstract class AbstractPipeListeningQueue extends AbstractSerializableListeningQueue<Event> {

  int referenceCount = 0;

  protected AbstractPipeListeningQueue(LinkedQueueSerializerType serializerType) {
    super(serializerType);
  }

  public synchronized void increaseReferenceCount() {
    referenceCount++;
    if (referenceCount == 1) {
      open();
    }
  }

  public synchronized void decreaseReferenceCount() throws IOException {
    referenceCount--;
    if (referenceCount == 0) {
      close();
    }
  }

  public long findAvailableSnapshot() {
    try (ConcurrentIterableLinkedQueue<Event>.DynamicIterator itr = queue.iterateFromEarliest()) {
      Event event;
      do {
        // Return immediately
        event = itr.next(0);
        // TODO: configure max event num to allow transferring existing snapshot
      } while (!(event instanceof PipeSnapshotEvent)
          && itr.getNextIndex() < queue.getTailIndex() - 1000
          && !Objects.isNull(event));
      return event instanceof PipeSnapshotEvent ? itr.getNextIndex() - 1 : Long.MAX_VALUE;
    }
  }
}
