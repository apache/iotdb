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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileDeviceIterator;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CompactionEstimateUtils {

  /**
   * Get the details of the tsfile, the returned array contains the following elements in sequence:
   *
   * <p>total chunk num in this tsfile
   *
   * <p>max chunk num of one timeseries in this tsfile
   *
   * <p>max aligned series num in one device. If there is no aligned series in this file, then it
   * turns to be -1.
   *
   * <p>max chunk num of one device in this tsfile
   *
   * @throws IOException if io errors occurred
   */
  public static FileInfo calculateFileInfo(TsFileSequenceReader reader) throws IOException {
    int totalChunkNum = 0;
    int maxChunkNum = 0;
    int maxAlignedSeriesNumInDevice = -1;
    int maxDeviceChunkNum = 0;
    TsFileDeviceIterator deviceIterator = reader.getAllDevicesIteratorWithIsAligned();
    while (deviceIterator.hasNext()) {
      int deviceChunkNum = 0;
      int alignedSeriesNumInDevice = 0;

      Pair<String, Boolean> deviceWithIsAlignedPair = deviceIterator.next();
      String device = deviceWithIsAlignedPair.left;
      boolean isAlignedDevice = deviceWithIsAlignedPair.right;

      Iterator<Map<String, List<ChunkMetadata>>> measurementChunkMetadataListMapIterator =
          reader.getMeasurementChunkMetadataListMapIterator(device);
      while (measurementChunkMetadataListMapIterator.hasNext()) {
        Map<String, List<ChunkMetadata>> measurementChunkMetadataListMap =
            measurementChunkMetadataListMapIterator.next();
        if (isAlignedDevice) {
          alignedSeriesNumInDevice += measurementChunkMetadataListMap.size();
        }

        for (Map.Entry<String, List<ChunkMetadata>> measurementChunkMetadataList :
            measurementChunkMetadataListMap.entrySet()) {
          int currentChunkMetadataListSize = measurementChunkMetadataList.getValue().size();
          deviceChunkNum += currentChunkMetadataListSize;
          totalChunkNum += currentChunkMetadataListSize;
          maxChunkNum = Math.max(maxChunkNum, currentChunkMetadataListSize);
        }
      }
      if (isAlignedDevice) {
        maxAlignedSeriesNumInDevice =
            Math.max(maxAlignedSeriesNumInDevice, alignedSeriesNumInDevice);
      }
      maxDeviceChunkNum = Math.max(maxDeviceChunkNum, deviceChunkNum);
    }
    long averageChunkMetadataSize =
        totalChunkNum == 0 ? 0 : reader.getAllMetadataSize() / totalChunkNum;
    return new FileInfo(
        totalChunkNum,
        maxChunkNum,
        maxAlignedSeriesNumInDevice,
        maxDeviceChunkNum,
        averageChunkMetadataSize);
  }

  public static boolean addReadLock(List<TsFileResource> resources) {
    for (int i = 0; i < resources.size(); i++) {
      TsFileResource resource = resources.get(i);
      resource.readLock();
      if (resource.isDeleted()) {
        // release read lock
        for (int j = 0; j <= i; j++) {
          resources.get(j).readUnlock();
        }
        return false;
      }
    }
    return true;
  }

  public static void releaseReadLock(List<TsFileResource> resources) {
    resources.forEach(TsFileResource::readUnlock);
  }
}
