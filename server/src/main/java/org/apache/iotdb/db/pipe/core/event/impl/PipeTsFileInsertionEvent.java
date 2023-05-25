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

package org.apache.iotdb.db.pipe.core.event.impl;

import org.apache.iotdb.commons.consensus.index.ConsensusIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumConsensusIndex;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeTsFileInsertionEvent extends EnrichedEvent implements TsFileInsertionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileInsertionEvent.class);

  private final TsFileResource resource;
  private File tsFile;
  private final AtomicBoolean isClosed;

  public PipeTsFileInsertionEvent(TsFileResource resource) {
    this.resource = resource;
    tsFile = resource.getTsFile();

    isClosed = new AtomicBoolean(resource.isClosed());
    // register close listener if TsFile is not closed
    if (!isClosed.get()) {
      final TsFileProcessor processor = resource.getProcessor();
      if (processor != null) {
        processor.addCloseFileListener(
            o -> {
              synchronized (isClosed) {
                isClosed.set(true);
                isClosed.notifyAll();
              }
            });
      }
    }
  }

  public void waitForTsFileClose() throws InterruptedException {
    if (!isClosed.get()) {
      synchronized (isClosed) {
        while (!isClosed.get()) {
          isClosed.wait();
        }
      }
    }
  }

  public File getTsFile() {
    return tsFile;
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public TsFileInsertionEvent toTsFileInsertionEvent(Iterable<TabletInsertionEvent> iterable) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean increaseResourceReferenceCount(String holderMessage) {
    try {
      // TODO: increase reference count for mods & resource files
      tsFile = PipeResourceManager.file().increaseFileReference(tsFile, true);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for TsFile %s error. Holder Message: %s",
              tsFile.getPath(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean decreaseResourceReferenceCount(String holderMessage) {
    try {
      PipeResourceManager.file().decreaseFileReference(tsFile);
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for TsFile %s error. Holder Message: %s",
              tsFile.getPath(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public ConsensusIndex getConsensusIndex() {
    try {
      waitForTsFileClose();
      return resource.getMaxConsensusIndexAfterClose();
    } catch (InterruptedException e) {
      LOGGER.warn(
          String.format(
              "Interrupted when waiting for closing TsFile %s.", resource.getTsFilePath()));
      return new MinimumConsensusIndex();
    }
  }

  @Override
  public PipeTsFileInsertionEvent shallowCopySelf() {
    return new PipeTsFileInsertionEvent(this.resource);
  }

  @Override
  public String toString() {
    return "PipeTsFileInsertionEvent{" + "tsFile=" + tsFile + '}';
  }
}
