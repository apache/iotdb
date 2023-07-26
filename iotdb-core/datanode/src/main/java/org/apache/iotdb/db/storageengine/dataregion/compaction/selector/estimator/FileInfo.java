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

public class FileInfo {
  // total chunk num in this tsfile
  int totalChunkNum = 0;
  // max chunk num of one timeseries in this tsfile
  int maxSeriesChunkNum = 0;
  // max aligned series num in one device. If there is no aligned series in this file, then it
  // turns to be -1.
  int maxAlignedSeriesNumInDevice = -1;
  // max chunk num of one device in this tsfile
  @SuppressWarnings("squid:S1068")
  int maxDeviceChunkNum = 0;

  long averageChunkMetadataSize = 0;

  public FileInfo(
      int totalChunkNum,
      int maxSeriesChunkNum,
      int maxAlignedSeriesNumInDevice,
      int maxDeviceChunkNum,
      long averageChunkMetadataSize) {
    this.totalChunkNum = totalChunkNum;
    this.maxSeriesChunkNum = maxSeriesChunkNum;
    this.maxAlignedSeriesNumInDevice = maxAlignedSeriesNumInDevice;
    this.maxDeviceChunkNum = maxDeviceChunkNum;
    this.averageChunkMetadataSize = averageChunkMetadataSize;
  }
}
