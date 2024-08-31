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
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;

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

  private static final Set<PipeTsFileInsertionEventPhantomReference>
      pipeTsFileInsertionEventPhantomRefs = ConcurrentHashMap.newKeySet();
  private static final ReferenceQueue<EnrichedEvent> referenceQueue = new ReferenceQueue<>();

  public PipePhantomReferenceManager() {
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob("PipeTsFileResourceManager#ttlCheck()", this::gcHook, 10);
  }

  private void gcHook() {
    Reference<? extends EnrichedEvent> reference;
    try {
      while ((reference = referenceQueue.remove(500)) != null) {
        finalizeResource((PipeTsFileInsertionEventPhantomReference) reference);
      }
    } catch (InterruptedException e) {
      // Finalize remaining references.
      while ((reference = referenceQueue.poll()) != null) {
        finalizeResource((PipeTsFileInsertionEventPhantomReference) reference);
      }
      pipeTsFileInsertionEventPhantomRefs.clear();
    } catch (Exception ex) {
      // Nowhere to really log this.
    }
  }

  public void trackPipeTsFileInsertionEventResources(
      PipeTsFileInsertionEvent event, PipeTsFileInsertionEventResource resource) {
    PipeTsFileInsertionEventPhantomReference reference =
        new PipeTsFileInsertionEventPhantomReference(event, resource, referenceQueue);
    pipeTsFileInsertionEventPhantomRefs.add(reference);
  }

  private void finalizeResource(PipeTsFileInsertionEventPhantomReference reference) {
    try {
      reference.finalizeResources();
      reference.clear();
    } finally {
      pipeTsFileInsertionEventPhantomRefs.remove(reference);
    }
  }

  private static class PipeTsFileInsertionEventPhantomReference
      extends PhantomReference<EnrichedEvent> {

    private PipeTsFileInsertionEventResource resource;

    public PipeTsFileInsertionEventPhantomReference(
        final EnrichedEvent event,
        final PipeTsFileInsertionEventResource resource,
        final ReferenceQueue<? super EnrichedEvent> queue) {
      super(event, queue);
      this.resource = resource;
    }

    void finalizeResources() {
      if (this.resource != null) {
        try {
          this.resource.forceClose();
        } finally {
          this.resource = null;
        }
      }
    }
  }

  public static class PipeTsFileInsertionEventResource {

    private final AtomicBoolean isReleased;
    private final AtomicInteger referenceCount;
    private final File tsFile;
    private final boolean isWithMod;
    private final File modFile;

    public PipeTsFileInsertionEventResource(
        AtomicBoolean isReleased,
        final AtomicInteger referenceCount,
        File tsFile,
        boolean isWithMod,
        File modFile) {
      this.isReleased = isReleased;
      this.referenceCount = referenceCount;
      this.tsFile = tsFile;
      this.isWithMod = isWithMod;
      this.modFile = modFile;
    }

    public void forceClose() {
      LOGGER.info("Force close TsFile {}", tsFile.getPath());
      if (isReleased.get()) {
        return;
      }

      if (referenceCount.get() >= 1) {
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

      referenceCount.set(0);
      isReleased.set(true);
    }
  }
}
