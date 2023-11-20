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

package org.apache.iotdb.db.pipe.resource.tsfile;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileReader;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTsFileResource implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileResource.class);

  private final File hardlinkOrCopiedFile;
  private final boolean isTsFile;

  public static final long TSFILE_MIN_TIME_TO_LIVE_IN_MS = 1000L * 20;
  private final AtomicInteger referenceCount;
  private final AtomicLong lastUnpinToZeroTime;

  private PipeMemoryBlock allocatedMemoryBlock;
  private TsFileSequenceReader sequenceReader = null;
  private TsFileReader reader = null;
  private Map<String, List<String>> deviceMeasurementsMap = null;
  private Map<String, Boolean> deviceIsAlignedMap = null;
  private Map<String, TSDataType> measurementDataTypeMap = null;

  public PipeTsFileResource(File hardlinkOrCopiedFile, boolean isTsFile) {
    this.hardlinkOrCopiedFile = hardlinkOrCopiedFile;
    this.isTsFile = isTsFile;

    referenceCount = new AtomicInteger(1);
    lastUnpinToZeroTime = new AtomicLong(Long.MAX_VALUE);
  }

  public File getFile() {
    return hardlinkOrCopiedFile;
  }

  ///////////////////// Reference Count /////////////////////

  public int getReferenceCount() {
    return referenceCount.get();
  }

  public int increaseAndGetReference() {
    return referenceCount.addAndGet(1);
  }

  public int decreaseAndGetReference() {
    final int finalReferenceCount = referenceCount.addAndGet(-1);
    if (finalReferenceCount == 0) {
      lastUnpinToZeroTime.set(System.currentTimeMillis());
    }
    if (finalReferenceCount < 0) {
      LOGGER.warn("PipeTsFileResource's reference count is decreased to below 0.");
    }
    return finalReferenceCount;
  }

  public synchronized boolean closeIfOutOfTimeToLive() throws IOException {
    if (referenceCount.get() <= 0
        && (sequenceReader == null // Not cached yet.
            || System.currentTimeMillis() - lastUnpinToZeroTime.get()
                > TSFILE_MIN_TIME_TO_LIVE_IN_MS)) {
      close();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }

    if (sequenceReader != null) {
      sequenceReader.close();
      sequenceReader = null;
    }

    if (deviceMeasurementsMap != null) {
      deviceMeasurementsMap = null;
    }

    if (deviceIsAlignedMap != null) {
      deviceIsAlignedMap = null;
    }

    if (measurementDataTypeMap != null) {
      measurementDataTypeMap = null;
    }

    if (allocatedMemoryBlock != null) {
      allocatedMemoryBlock.close();
      allocatedMemoryBlock = null;
    }

    Files.deleteIfExists(hardlinkOrCopiedFile.toPath());

    LOGGER.info("PipeTsFileResource: Closed tsfile {} and cleaned up.", hardlinkOrCopiedFile);
  }

  //////////////////////////// Cache Getter ////////////////////////////

  public synchronized TsFileSequenceReader tryGetTsFileSequenceReader() throws IOException {
    if (sequenceReader == null && isTsFile) {
      tryCacheObjects();
    }
    return sequenceReader;
  }

  public synchronized TsFileReader tryGetTsFileReader() throws IOException {
    if (reader == null && isTsFile) {
      tryCacheObjects();
    }
    return reader;
  }

  public synchronized Map<String, List<String>> tryGetDeviceMeasurementsMap() throws IOException {
    if (deviceMeasurementsMap == null && isTsFile) {
      tryCacheObjects();
    }
    return deviceMeasurementsMap;
  }

  public synchronized Map<String, Boolean> tryGetDeviceIsAlignedMap() throws IOException {
    if (deviceIsAlignedMap == null && isTsFile) {
      tryCacheObjects();
    }
    return deviceIsAlignedMap;
  }

  public synchronized Map<String, TSDataType> tryGetMeasurementDataTypeMap() throws IOException {
    if (measurementDataTypeMap == null && isTsFile) {
      tryCacheObjects();
    }
    return measurementDataTypeMap;
  }

  synchronized boolean tryCacheObjects() throws IOException {
    if (!isTsFile) {
      return false;
    }

    // Only allocate when pipe memory used is less than 50%, because
    // memory for tsfileReader is hard to shrink and may eat up too much memory.
    allocatedMemoryBlock =
        PipeResourceManager.memory()
            .forceAllocateIfSufficient(
                PipeConfig.getInstance().getPipeMemoryAllocateForTsFileSequenceReaderInBytes(),
                0.5f);
    if (allocatedMemoryBlock == null) {
      return false;
    }

    LOGGER.info(
        "PipeTsFileResource: Cached objects for tsfile {}.", hardlinkOrCopiedFile.getPath());

    sequenceReader = new TsFileSequenceReader(hardlinkOrCopiedFile.getPath(), true, true);
    reader = new TsFileReader(sequenceReader);
    deviceMeasurementsMap = sequenceReader.getDeviceMeasurementsMap();
    deviceIsAlignedMap = new HashMap<>();
    final TsFileDeviceIterator deviceIsAlignedIterator =
        sequenceReader.getAllDevicesIteratorWithIsAligned();
    while (deviceIsAlignedIterator.hasNext()) {
      final Pair<String, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
      deviceIsAlignedMap.put(deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
    }
    measurementDataTypeMap = sequenceReader.getFullPathDataTypeMap();

    // Clear cached device metadata to save memory.
    sequenceReader.clearCachedDeviceMetadata();

    return true;
  }
}
