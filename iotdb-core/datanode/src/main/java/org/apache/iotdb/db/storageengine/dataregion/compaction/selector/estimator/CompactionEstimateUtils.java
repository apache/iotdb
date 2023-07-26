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
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
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
  public static FileInfo getSeriesAndDeviceChunkNum(TsFileSequenceReader reader)
      throws IOException {
    int totalChunkNum = 0;
    int maxChunkNum = 0;
    int maxAlignedSeriesNumInDevice = -1;
    int maxDeviceChunkNum = 0;
    Map<String, List<TimeseriesMetadata>> deviceMetadata = reader.getAllTimeseriesMetadata(true);
    for (Map.Entry<String, List<TimeseriesMetadata>> entry : deviceMetadata.entrySet()) {
      int deviceChunkNum = 0;
      List<TimeseriesMetadata> deviceTimeseriesMetadata = entry.getValue();
      if (deviceTimeseriesMetadata.get(0).getMeasurementId().equals("")) {
        // aligned device
        maxAlignedSeriesNumInDevice =
            Math.max(maxAlignedSeriesNumInDevice, deviceTimeseriesMetadata.size());
      }
      for (TimeseriesMetadata timeseriesMetadata : deviceTimeseriesMetadata) {
        deviceChunkNum += timeseriesMetadata.getChunkMetadataList().size();
        totalChunkNum += timeseriesMetadata.getChunkMetadataList().size();
        maxChunkNum = Math.max(maxChunkNum, timeseriesMetadata.getChunkMetadataList().size());
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
