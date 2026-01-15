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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionPathUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.read.LazyTsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.Optional;

public class TreeDiskUsageStatisticUtil extends DiskUsageStatisticUtil {

  public static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TreeDiskUsageStatisticUtil.class);

  private final PartialPath pathPattern;
  private final boolean isMatchedDeviceSequential;
  private long result;

  public TreeDiskUsageStatisticUtil(
      TsFileManager tsFileManager,
      long timePartition,
      PartialPath pathPattern,
      Optional<FragmentInstanceContext> context) {
    super(tsFileManager, timePartition, context);
    this.pathPattern = pathPattern;
    this.result = 0;
    String[] nodes = pathPattern.getNodes();
    boolean hasWildcardInPath = false;
    for (int i = 0; i < nodes.length; i++) {
      if (nodes[i].equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
          || nodes[i].equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        hasWildcardInPath = true;
        continue;
      }
      if (hasWildcardInPath) {
        this.isMatchedDeviceSequential = false;
        return;
      }
    }
    this.isMatchedDeviceSequential = true;
  }

  public long[] getResult() {
    return new long[] {result};
  }

  @Override
  protected boolean calculateWithoutOpenFile(TsFileResource tsFileResource) {
    return false;
  }

  @Override
  protected void calculateNextFile(TsFileResource tsFileResource, TsFileSequenceReader reader)
      throws IOException, IllegalPathException {
    long firstDeviceMeasurementNodeOffset = -1;
    LazyTsFileDeviceIterator deviceIterator = reader.getLazyDeviceIterator();
    while (deviceIterator.hasNext()) {
      IDeviceID deviceIsAlignedPair = deviceIterator.next();
      firstDeviceMeasurementNodeOffset =
          firstDeviceMeasurementNodeOffset == -1
              ? deviceIterator.getCurrentDeviceMeasurementNodeOffset()[0]
              : firstDeviceMeasurementNodeOffset;
      if (!matchPathPattern(deviceIsAlignedPair)) {
        continue;
      }
      MetadataIndexNode nodeOfFirstMatchedDevice =
          deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
      addMeasurementNodeSizeForCurrentDevice(deviceIterator);
      IDeviceID nextNotMatchedDevice = null;
      MetadataIndexNode nodeOfNextNotMatchedDevice = null;
      while (deviceIterator.hasNext()) {
        IDeviceID currentDevice = deviceIterator.next();
        if (!matchPathPattern(currentDevice)) {
          nextNotMatchedDevice = currentDevice;
          nodeOfNextNotMatchedDevice = deviceIterator.getFirstMeasurementNodeOfCurrentDevice();
          break;
        }
        addMeasurementNodeSizeForCurrentDevice(deviceIterator);
      }
      result +=
          calculatePathPatternSize(
              reader,
              new Pair<>(deviceIsAlignedPair, reader.isAlignedDevice(nodeOfFirstMatchedDevice)),
              nodeOfFirstMatchedDevice,
              nodeOfNextNotMatchedDevice == null
                  ? null
                  : new Pair<>(
                      nextNotMatchedDevice, reader.isAlignedDevice(nodeOfNextNotMatchedDevice)),
              nodeOfNextNotMatchedDevice,
              firstDeviceMeasurementNodeOffset);
      if (isMatchedDeviceSequential) {
        break;
      }
    }
  }

  private void addMeasurementNodeSizeForCurrentDevice(LazyTsFileDeviceIterator iterator) {
    long[] startEndPair = iterator.getCurrentDeviceMeasurementNodeOffset();
    result += startEndPair[1] - startEndPair[0];
  }

  private long calculatePathPatternSize(
      TsFileSequenceReader reader,
      Pair<IDeviceID, Boolean> firstMatchedDevice,
      MetadataIndexNode nodeOfFirstMatchedDevice,
      Pair<IDeviceID, Boolean> nextNotMatchedDevice,
      MetadataIndexNode nodeOfNextNotMatchedDevice,
      long firstDeviceMeasurementNodeOffset)
      throws IOException {
    Offsets chunkGroupTimeseriesMetadataStartOffsetPair, chunkGroupTimeseriesMetadataEndOffsetPair;
    if (nextNotMatchedDevice == null) {
      chunkGroupTimeseriesMetadataEndOffsetPair =
          new Offsets(
              reader.readFileMetadata().getMetaOffset(), firstDeviceMeasurementNodeOffset, 0);
    } else {
      chunkGroupTimeseriesMetadataEndOffsetPair =
          calculateStartOffsetOfChunkGroupAndTimeseriesMetadata(
              reader, nodeOfNextNotMatchedDevice, nextNotMatchedDevice, 0);
    }
    chunkGroupTimeseriesMetadataStartOffsetPair =
        calculateStartOffsetOfChunkGroupAndTimeseriesMetadata(
            reader, nodeOfFirstMatchedDevice, firstMatchedDevice, 0);
    return chunkGroupTimeseriesMetadataEndOffsetPair.minusOffsetForTreeModel(
        chunkGroupTimeseriesMetadataStartOffsetPair);
  }

  private boolean matchPathPattern(IDeviceID deviceID) throws IllegalPathException {
    return pathPattern.matchFullPath(CompactionPathUtils.getPath(deviceID));
  }
}
