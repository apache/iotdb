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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionPathUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;

public class TreeDiskUsageStatisticUtil extends DiskUsageStatisticUtil {

  private final PartialPath pathPattern;
  private final boolean isPrefixPathPattern;
  private long result;

  public TreeDiskUsageStatisticUtil(
      TsFileManager tsFileManager, long timePartition, PartialPath pathPattern) {
    super(tsFileManager, timePartition);
    this.pathPattern = pathPattern;
    this.isPrefixPathPattern = pathPattern.isPrefixPath();
    this.result = 0;
  }

  @Override
  public long[] getResult() {
    return new long[] {result};
  }

  @Override
  public void calculateNextFile() {
    TsFileResource tsFileResource = iterator.next();
    if (tsFileResource.isDeleted()) {
      return;
    }

    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath())) {
      TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
      while (deviceIterator.hasNext()) {
        Pair<IDeviceID, Boolean> deviceIsAlignedPair = deviceIterator.next();
        if (!matchPathPattern(deviceIsAlignedPair.getLeft())) {
          continue;
        }
        MetadataIndexNode nodeOfFirstMatchedDevice =
            deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
        Pair<IDeviceID, Boolean> nextNotMatchedDevice = null;
        MetadataIndexNode nodeOfNextNotMatchedDevice = null;
        while (deviceIterator.hasNext()) {
          Pair<IDeviceID, Boolean> currentDevice = deviceIterator.next();
          if (!matchPathPattern(currentDevice.getLeft())) {
            nextNotMatchedDevice = currentDevice;
            nodeOfNextNotMatchedDevice = deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
            break;
          }
        }
        result +=
            calculatePathPatternSize(
                reader,
                deviceIsAlignedPair,
                nodeOfFirstMatchedDevice,
                nextNotMatchedDevice,
                nodeOfNextNotMatchedDevice);
        if (isPrefixPathPattern) {
          break;
        }
      }
    } catch (Exception e) {
      logger.error("Failed to scan file {}", tsFileResource.getTsFile().getAbsolutePath(), e);
    }
  }

  private long calculatePathPatternSize(
      TsFileSequenceReader reader,
      Pair<IDeviceID, Boolean> firstMatchedDevice,
      MetadataIndexNode nodeOfFirstMatchedDevice,
      Pair<IDeviceID, Boolean> nextNotMatchedDevice,
      MetadataIndexNode nodeOfNextNotMatchedDevice)
      throws IOException {
    long startOffset, endOffset;
    if (nextNotMatchedDevice == null) {
      endOffset = reader.readFileMetadata().getMetaOffset();
    } else {
      endOffset =
          calculateStartOffsetOfChunkGroup(
              reader, nodeOfNextNotMatchedDevice, nextNotMatchedDevice);
    }
    startOffset =
        calculateStartOffsetOfChunkGroup(reader, nodeOfFirstMatchedDevice, firstMatchedDevice);
    return endOffset - startOffset;
  }

  private boolean matchPathPattern(IDeviceID deviceID) throws IllegalPathException {
    return pathPattern.matchFullPath(CompactionPathUtils.getPath(deviceID));
  }
}
