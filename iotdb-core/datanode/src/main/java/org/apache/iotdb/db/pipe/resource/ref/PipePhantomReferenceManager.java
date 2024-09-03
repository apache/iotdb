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

package org.apache.iotdb.db.pipe.resource.ref;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PipePhantomReferenceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePhantomReferenceManager.class);

  private static final Set<PipeEventPhantomReference> PIPE_EVENT_PHANTOM_REFERENCES =
      ConcurrentHashMap.newKeySet();

  private static final ReferenceQueue<EnrichedEvent> REFERENCE_QUEUE = new ReferenceQueue<>();

  public PipePhantomReferenceManager() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob("PipePhantomReferenceManager#gcHook()", this::gcHook, 10);
  }

  private void gcHook() {
    Reference<? extends EnrichedEvent> reference;
    try {
      while ((reference = REFERENCE_QUEUE.remove(500)) != null) {
        finalizeResource((PipeEventPhantomReference) reference);
      }
    } catch (final InterruptedException e) {
      // Finalize remaining references.
      while ((reference = REFERENCE_QUEUE.poll()) != null) {
        finalizeResource((PipeEventPhantomReference) reference);
      }
      PIPE_EVENT_PHANTOM_REFERENCES.clear();
    } catch (final Exception e) {
      // Nowhere to really log this.
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

  ///////////////////// APIs provided for PipeTsFileInsertionEvent /////////////////////

  public void trackPipeTsFileInsertionEventResource(
      final PipeTsFileInsertionEvent event, final PipeTsFileInsertionEventResource resource) {
    final PipeEventPhantomReference reference =
        new PipeEventPhantomReference(event, resource, REFERENCE_QUEUE);
    PIPE_EVENT_PHANTOM_REFERENCES.add(reference);
  }

  public static class PipeTsFileInsertionEventResource extends PipeEventResource {

    private final File tsFile;
    private final boolean isWithMod;
    private final File modFile;

    public PipeTsFileInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final File tsFile,
        final boolean isWithMod,
        final File modFile) {
      super(isReleased, referenceCount);
      this.tsFile = tsFile;
      this.isWithMod = isWithMod;
      this.modFile = modFile;
    }

    @Override
    protected void finalizeResource() {
      try {
        PipeDataNodeResourceManager.tsfile().decreaseFileReference(tsFile);
        if (isWithMod) {
          PipeDataNodeResourceManager.tsfile().decreaseFileReference(modFile);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            String.format("Decrease reference count for TsFile %s error.", tsFile.getPath()), e);
      }
    }
  }

  ///////////////////// APIs provided for PipeInsertNodeTabletInsertionEvent /////////////////////

  public void trackPipeInsertNodeTabletInsertionEventResource(
      final PipeInsertNodeTabletInsertionEvent event,
      final PipeInsertNodeTabletInsertionEventResource resource) {
    final PipeEventPhantomReference reference =
        new PipeEventPhantomReference(event, resource, REFERENCE_QUEUE);
    PIPE_EVENT_PHANTOM_REFERENCES.add(reference);
  }

  public static class PipeInsertNodeTabletInsertionEventResource extends PipeEventResource {

    private final WALEntryHandler walEntryHandler;

    public PipeInsertNodeTabletInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final WALEntryHandler walEntryHandler) {
      super(isReleased, referenceCount);
      this.walEntryHandler = walEntryHandler;
    }

    @Override
    protected void finalizeResource() {
      try {
        PipeDataNodeResourceManager.wal().unpin(walEntryHandler);
        // no need to release the containers' memory because it has already been GCed
      } catch (final Exception e) {
        LOGGER.warn(
            String.format(
                "Decrease reference count for memTable %d error.", walEntryHandler.getMemTableId()),
            e);
      }
    }
  }

  ///////////////////// APIs provided for PipeRawTabletInsertionEventResource /////////////////////

  public void trackPipeRawTabletInsertionEventResource(
      final PipeRawTabletInsertionEvent event, final PipeRawTabletInsertionEventResource resource) {
    final PipeEventPhantomReference reference =
        new PipeEventPhantomReference(event, resource, REFERENCE_QUEUE);
    PIPE_EVENT_PHANTOM_REFERENCES.add(reference);
  }

  public static class PipeRawTabletInsertionEventResource extends PipeEventResource {

    private final PipeTabletMemoryBlock allocatedMemoryBlock;

    public PipeRawTabletInsertionEventResource(
        final AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        final PipeTabletMemoryBlock allocatedMemoryBlock) {
      super(isReleased, referenceCount);
      this.allocatedMemoryBlock = allocatedMemoryBlock;
    }

    @Override
    protected void finalizeResource() {
      allocatedMemoryBlock.close();
    }
  }

  /////////////////////////// Resource & PhantomReference ///////////////////////////

  private abstract static class PipeEventResource {

    private final AtomicBoolean isReleased;
    private final AtomicInteger referenceCount;

    private PipeEventResource(final AtomicBoolean isReleased, final AtomicInteger referenceCount) {
      this.isReleased = isReleased;
      this.referenceCount = referenceCount;
    }

    private void clearReferenceCount(final String holderMessage) {
      if (isReleased.get()) {
        return;
      }

      if (referenceCount.get() >= 1) {
        LOGGER.info("finalize resource for event with holder message {}", holderMessage);
        finalizeResource();
      }

      referenceCount.set(0);
      isReleased.set(true);
    }

    protected abstract void finalizeResource();
  }

  private static class PipeEventPhantomReference extends PhantomReference<EnrichedEvent> {

    private final String coreReportMessageSnapshot;
    private PipeEventResource resource;

    private PipeEventPhantomReference(
        final EnrichedEvent event,
        final PipeEventResource resource,
        final ReferenceQueue<? super EnrichedEvent> queue) {
      super(event, queue);
      this.coreReportMessageSnapshot = event.coreReportMessage();
      this.resource = resource;
    }

    private void finalizeResources() {
      if (this.resource != null) {
        try {
          this.resource.clearReferenceCount(coreReportMessageSnapshot);
        } finally {
          this.resource = null;
        }
      }
    }
  }
}
