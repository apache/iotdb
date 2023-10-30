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

package org.apache.iotdb.db.pipe.event.common.tsfile;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileBatchInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class PipeBatchTsFileInsertionEvent extends EnrichedEvent
    implements TsFileBatchInsertionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeBatchTsFileInsertionEvent.class);

  // used to filter data
  private final long startTime;
  private final long endTime;
  private final boolean needParseTime;

  private final List<TsFileResource> resources;
  private List<File> tsFiles;

  private final boolean isLoaded;
  private final boolean isGeneratedByPipe;

  private final AtomicBoolean[] isClosed;

  private TsFileListInsertionDataContainer dataContainer;

  public PipeBatchTsFileInsertionEvent(
      List<TsFileResource> resources, boolean isLoaded, boolean isGeneratedByPipe) {
    this(resources, isLoaded, isGeneratedByPipe, null, null, Long.MIN_VALUE, Long.MAX_VALUE, false);
  }

  public PipeBatchTsFileInsertionEvent(
      List<TsFileResource> resources,
      boolean isLoaded,
      boolean isGeneratedByPipe,
      PipeTaskMeta pipeTaskMeta,
      String pattern,
      long startTime,
      long endTime,
      boolean needParseTime) {
    super(pipeTaskMeta, pattern);

    this.startTime = startTime;
    this.endTime = endTime;
    this.needParseTime = needParseTime;

    if (needParseTime) {
      this.isTimeParsed = false;
    }

    this.resources = resources;
    tsFiles = resources.stream().map(TsFileResource::getTsFile).collect(Collectors.toList());
    isClosed = new AtomicBoolean[resources.size()];

    this.isLoaded = isLoaded;
    this.isGeneratedByPipe = isGeneratedByPipe;

    for (int i = 0; i < isClosed.length; i++) {
      TsFileResource resource = resources.get(i);
      isClosed[i] = new AtomicBoolean(resource.isClosed());
      if (!isClosed[i].get()) {
        final TsFileProcessor processor = resource.getProcessor();
        if (processor != null) {
          int finalI = i;
          processor.addCloseFileListener(
              o -> {
                synchronized (isClosed[finalI]) {
                  isClosed[finalI].set(true);
                  isClosed[finalI].notifyAll();
                }
              });
        }
      }
      // check again after register close listener in case TsFile is closed during the process
      isClosed[i].set(resource.isClosed());
    }
  }

  public void waitForTsFileClose() throws InterruptedException {
    for (AtomicBoolean signal : isClosed) {
      if (!signal.get()) {
        synchronized (signal) {
          while (!signal.get()) {
            isClosed.wait();
          }
        }
      }
    }
  }

  public List<File> getTsFiles() {
    return tsFiles;
  }

  public List<TsFileResource> getResources() {
    return resources;
  }

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    for (int i = 0; i < tsFiles.size(); i++) {
      File tsFile = tsFiles.get(i);
      try {
        tsFiles.set(i, PipeResourceManager.tsfile().increaseFileReference(tsFile, true));
      } catch (Exception e) {
        LOGGER.warn(
            String.format(
                "Increase reference count for TsFile %s error. Holder Message: %s",
                tsFile.getPath(), holderMessage),
            e);
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    for (File tsFile : tsFiles) {
      try {
        PipeResourceManager.tsfile().decreaseFileReference(tsFile);
      } catch (Exception e) {
        LOGGER.warn(
            String.format(
                "Decrease reference count for TsFile %s error. Holder Message: %s",
                tsFile.getPath(), holderMessage),
            e);
        return false;
      }
    }
    return true;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    try {
      waitForTsFileClose();
      return resources.get(resources.size() - 1).getMaxProgressIndexAfterClose();
    } catch (InterruptedException e) {
      LOGGER.warn(String.format("Interrupted when waiting for closing TsFiles %s.", resources));
      Thread.currentThread().interrupt();
      return MinimumProgressIndex.INSTANCE;
    }
  }

  @Override
  public PipeBatchTsFileInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeBatchTsFileInsertionEvent(
        resources,
        isLoaded,
        isGeneratedByPipe,
        pipeTaskMeta,
        pattern,
        startTime,
        endTime,
        needParseTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  /////////////////////////// TsFileInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    try {
      if (dataContainer == null) {
        waitForTsFileClose();
        dataContainer =
            new TsFileListInsertionDataContainer(
                tsFiles, getPattern(), startTime, endTime, pipeTaskMeta, this);
      }
      return dataContainer.toTabletInsertionEvents();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      close();

      final String errorMsg =
          String.format("Interrupted when waiting for closing TsFiles %s.", resources);
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg);
    } catch (IOException e) {
      close();

      final String errorMsg = String.format("Read TsFiles %s error.", resources);
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg);
    }
  }

  /** Release the resource of data container. */
  @Override
  public void close() {
    if (dataContainer != null) {
      dataContainer.close();
      dataContainer = null;
    }
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return "PipeTsFileInsertionEvent{"
        + "resources="
        + resources
        + ", tsFiles="
        + tsFiles
        + ", isClosed="
        + isClosed
        + '}';
  }

  public List<PipeTsFileInsertionEvent> toSingleFileEvents() {
    List<PipeTsFileInsertionEvent> result = new ArrayList<>();
    for (TsFileResource resource : resources) {
      result.add(
          new PipeTsFileInsertionEvent(
              resource,
              isLoaded,
              isGeneratedByPipe,
              pipeTaskMeta,
              getPattern(),
              startTime,
              endTime,
              isTsFileResourceCoveredByTimeRange(resource)));
    }
    return result;
  }

  protected boolean isTsFileResourceCoveredByTimeRange(TsFileResource resource) {
    return startTime <= resource.getFileStartTime() && endTime >= resource.getFileEndTime();
  }
}
