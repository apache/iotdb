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

package org.apache.iotdb.commons.pipe.resource.ref;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class PipePhantomReferenceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePhantomReferenceManager.class);

  private static final Set<PipeEventPhantomReference> PIPE_EVENT_PHANTOM_REFERENCES =
      ConcurrentHashMap.newKeySet();

  private static final ReferenceQueue<EnrichedEvent> REFERENCE_QUEUE = new ReferenceQueue<>();

  private volatile long lastPhantomReferenceCount = -1;

  public PipePhantomReferenceManager() {
    // Do nothing now.
  }

  public int getPhantomReferenceCount() {
    return PIPE_EVENT_PHANTOM_REFERENCES.size();
  }

  protected void gcHook() {
    if (!PipeConfig.getInstance().getPipeEventReferenceTrackingEnabled()) {
      return;
    }

    final long startTime = System.currentTimeMillis();

    // limit control to avoid infinite execution
    final int maxCount = getPhantomReferenceCount();
    int count = 0;

    Reference<? extends EnrichedEvent> reference;
    try {
      while (count < maxCount && ((reference = REFERENCE_QUEUE.remove(500)) != null)) {
        finalizeResource((PipeEventPhantomReference) reference);
        count++;
      }
    } catch (final InterruptedException e) {
      // Finalize remaining references.
      while (count < maxCount && ((reference = REFERENCE_QUEUE.poll()) != null)) {
        finalizeResource((PipeEventPhantomReference) reference);
        count++;
      }
    } catch (final Exception e) {
      // Nowhere to really log this.
    }

    if (count != 0) {
      LOGGER.info(
          "Clean {} pipe phantom reference(s) successfully within {} ms, remaining reference count: {}",
          count,
          System.currentTimeMillis() - startTime,
          getPhantomReferenceCount());
    } else {
      final long currentPhantomReferenceCount = getPhantomReferenceCount();
      if (currentPhantomReferenceCount != lastPhantomReferenceCount) {
        if (lastPhantomReferenceCount != -1) {
          LOGGER.info("Remaining pipe phantom reference count: {}", currentPhantomReferenceCount);
        }
        lastPhantomReferenceCount = currentPhantomReferenceCount;
      }
    }
  }

  private void finalizeResource(final PipeEventPhantomReference reference) {
    try {
      reference.finalizeResources();
      reference.clear();
    } finally {
      PIPE_EVENT_PHANTOM_REFERENCES.remove(reference);
    }
  }

  private static class PipeEventPhantomReference extends PhantomReference<EnrichedEvent> {

    private final String holderMessage;
    private PipeEventResource resource;

    private PipeEventPhantomReference(
        final EnrichedEvent event,
        final PipeEventResource resource,
        final ReferenceQueue<? super EnrichedEvent> queue) {
      super(event, queue);
      this.holderMessage = event.getClass().getSimpleName();
      this.resource = resource;
    }

    private void finalizeResources() {
      if (this.resource != null) {
        try {
          this.resource.clearReferenceCount(holderMessage);
        } finally {
          this.resource = null;
        }
      }
    }
  }

  ///////////////////// APIs provided for EnrichedEvent /////////////////////

  public void trackPipeEventResource(final EnrichedEvent event, final PipeEventResource resource) {
    final PipeEventPhantomReference reference =
        new PipeEventPhantomReference(event, resource, REFERENCE_QUEUE);
    PIPE_EVENT_PHANTOM_REFERENCES.add(reference);
  }

  public abstract static class PipeEventResource {

    private final AtomicBoolean isReleased;
    private final AtomicInteger referenceCount;

    protected PipeEventResource(
        final AtomicBoolean isReleased, final AtomicInteger referenceCount) {
      this.isReleased = isReleased;
      this.referenceCount = referenceCount;
    }

    private void clearReferenceCount(final String holderMessage) {
      if (isReleased.get()) {
        return;
      }

      if (referenceCount.get() >= 1) {
        LOGGER.error("PIPE EVENT RESOURCE LEAK DETECTED: {}", holderMessage);
        finalizeResource();
      }

      referenceCount.set(0);
      isReleased.set(true);
    }

    protected abstract void finalizeResource();
  }
}
