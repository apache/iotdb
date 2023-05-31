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

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.pipe.core.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.core.event.utils.TabletIterator;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeTsFileInsertionEvent extends EnrichedEvent implements TsFileInsertionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileInsertionEvent.class);

  private final TsFileResource resource;
  private File tsFile;

  private final AtomicBoolean isClosed;

  private final Map<String, List<TimeseriesMetadata>> device2TimeseriesMetadataMap;

  public PipeTsFileInsertionEvent(TsFileResource resource) {
    this(resource, null, null);
  }

  public PipeTsFileInsertionEvent(
      TsFileResource resource, PipeTaskMeta pipeTaskMeta, String pattern) {
    super(pipeTaskMeta, pattern);

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

    this.device2TimeseriesMetadataMap = matchPattern();
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

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean increaseResourceReferenceCount(String holderMessage) {
    try {
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
  public ProgressIndex getProgressIndex() {
    try {
      waitForTsFileClose();
      return resource.getMaxProgressIndexAfterClose();
    } catch (InterruptedException e) {
      LOGGER.warn(
          String.format(
              "Interrupted when waiting for closing TsFile %s.", resource.getTsFilePath()));
      Thread.currentThread().interrupt();
      return new MinimumProgressIndex();
    }
  }

  @Override
  public String getPattern() {
    return pattern;
  }

  @Override
  public PipeTsFileInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return new PipeTsFileInsertionEvent(resource, pipeTaskMeta, pattern);
  }

  /////////////////////////// TsFileInsertionEvent ///////////////////////////

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return () ->
        new Iterator<TabletInsertionEvent>() {
          private Iterator<Tablet> tabletIterator = readTsFile().iterator();

          @Override
          public boolean hasNext() {
            return tabletIterator.hasNext();
          }

          @Override
          public TabletInsertionEvent next() {
            return new PipeTabletInsertionEvent(tabletIterator.next());
          }
        };
  }

  @Override
  public TsFileInsertionEvent toTsFileInsertionEvent(Iterable<TabletInsertionEvent> iterable) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private Map<String, List<TimeseriesMetadata>> matchPattern() {
    Map<String, List<TimeseriesMetadata>> result = new HashMap<>();
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getPath())) {

      // match pattern
      for (Map.Entry<String, List<TimeseriesMetadata>> entry :
          reader.getAllTimeseriesMetadata(true).entrySet()) {
        String device = entry.getKey();

        // case 1: for example, pattern is root.a.b or pattern is null and device is root.a.b.c
        // in this case, all data can be matched without checking the measurements
        if (pattern == null || pattern.length() <= device.length() && device.startsWith(pattern)) {
          result.put(device, entry.getValue());
        }

        // case 2: for example, pattern is root.a.b.c and device is root.a.b
        // in this case, we need to check the full path
        else {
          List<TimeseriesMetadata> timeseriesMetadataList = new ArrayList<>();
          for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {

            if (timeseriesMetadata.getTSDataType() == TSDataType.VECTOR) {
              timeseriesMetadataList.add(timeseriesMetadata);
              continue;
            }

            String measurement = timeseriesMetadata.getMeasurementId();
            // low cost check comes first
            if (pattern.length() == measurement.length() + device.length() + 1
                // high cost check comes later
                && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
              timeseriesMetadataList.add(timeseriesMetadata);
            }
          }
          result.put(device, timeseriesMetadataList);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Cannot read TsFile {}.", tsFile.getPath(), e);
    }
    return result;
  }

  private Iterable<Tablet> readTsFile() {
    return () -> {
      try {
        TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getPath());
        return new TabletIterator(reader, device2TimeseriesMetadataMap);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  };

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return "PipeTsFileInsertionEvent{"
        + "resource="
        + resource
        + ", tsFile="
        + tsFile
        + ", isClosed="
        + isClosed
        + '}';
  }
}
