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

  public static final long TSFILE_MIN_TIME_TO_LIVE_IN_MS = 1000L * 10;
  private final AtomicLong lastUnpinToZeroTime = new AtomicLong(Long.MAX_VALUE);
  private final File hardlinkOrCopiedFile;
  private final AtomicInteger referenceCount;
  private final boolean isTsFile;
  private TsFileSequenceReader sequenceReader = null;
  private TsFileReader reader = null;
  private Map<String, List<String>> deviceMeasurementsMap = null;
  private Map<String, Boolean> deviceIsAlignedMap = null;
  private Map<String, TSDataType> measurementDataTypeMap = null;

  public PipeTsFileResource(File hardlinkOrCopiedFile, boolean isTsFile) {
    this.hardlinkOrCopiedFile = hardlinkOrCopiedFile;
    this.referenceCount = new AtomicInteger(1);
    this.isTsFile = isTsFile;
  }

  public void initCachedObjects() throws IOException {
    if (isTsFile) {
      LOGGER.info("Creating TsFileReader for file: {}", hardlinkOrCopiedFile.getPath());
      sequenceReader = new TsFileSequenceReader(hardlinkOrCopiedFile.getPath(), true, true);
      reader = new TsFileReader(sequenceReader);
      deviceMeasurementsMap = sequenceReader.getDeviceMeasurementsMapIntern();
      deviceIsAlignedMap = new HashMap<>();
      final TsFileDeviceIterator deviceIsAlignedIterator =
          sequenceReader.getAllDevicesIteratorWithIsAligned();
      while (deviceIsAlignedIterator.hasNext()) {
        final Pair<String, Boolean> deviceIsAlignedPair = deviceIsAlignedIterator.next();
        deviceIsAlignedMap.put(deviceIsAlignedPair.getLeft(), deviceIsAlignedPair.getRight());
      }
      measurementDataTypeMap = sequenceReader.getFullPathDataTypeMapIntern();
      sequenceReader.clearCachedDeviceMetadata();
    }
  }

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

  public boolean closeIfOutOfTimeToLive() throws IOException {
    if (referenceCount.get() <= 0
        && (System.currentTimeMillis() - lastUnpinToZeroTime.get()
            > TSFILE_MIN_TIME_TO_LIVE_IN_MS)) {
      close();
      return true;
    } else {
      return false;
    }
  }

  public File getFile() {
    return hardlinkOrCopiedFile;
  }

  public TsFileSequenceReader getTsFileSequenceReader() throws IOException {
    if (sequenceReader == null && isTsFile) {
      initCachedObjects();
    }
    return sequenceReader;
  }

  public TsFileReader getTsFileReader() throws IOException {
    if (reader == null && isTsFile) {
      initCachedObjects();
    }
    return reader;
  }

  public Map<String, List<String>> getDeviceMeasurementsMap() throws IOException {
    if (deviceMeasurementsMap == null && isTsFile) {
      initCachedObjects();
    }
    return deviceMeasurementsMap;
  }

  public Map<String, Boolean> getDeviceIsAlignedMap() throws IOException {
    if (deviceIsAlignedMap == null && isTsFile) {
      initCachedObjects();
    }
    return deviceIsAlignedMap;
  }

  public Map<String, TSDataType> getMeasurementDataTypeMap() throws IOException {
    if (measurementDataTypeMap == null && isTsFile) {
      initCachedObjects();
    }
    return measurementDataTypeMap;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
    if (sequenceReader != null) {
      sequenceReader.close();
    }
    Files.deleteIfExists(hardlinkOrCopiedFile.toPath());
  }
}
