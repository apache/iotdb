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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.common.TimeRange;

public class CompactionStatisticsCheckFailedException extends RuntimeException {

  public CompactionStatisticsCheckFailedException(String msg) {
    super(msg);
  }

  public CompactionStatisticsCheckFailedException(
      IDeviceID deviceID, TimeRange deviceTimeRange, TimeRange actualDeviceTimeRange) {
    super(
        getExceptionMsg(
            deviceID,
            String.format(
                StorageEngineMessages.CURRENT_DEVICE_TIME_RANGE_MISMATCH_FMT,
                deviceTimeRange,
                actualDeviceTimeRange)));
  }

  public CompactionStatisticsCheckFailedException(
      IDeviceID deviceID, TimeseriesMetadata timeseriesMetadata, TimeRange actualTimeRange) {
    super(
        getExceptionMsg(
            deviceID,
            String.format(
                StorageEngineMessages.CURRENT_TIMESERIES_METADATA_MISMATCH_FMT,
                timeseriesMetadata,
                actualTimeRange)));
  }

  public CompactionStatisticsCheckFailedException(
      IDeviceID deviceID, ChunkMetadata chunkMetadata, TimeRange actualChunkTimeRange) {
    super(
        getExceptionMsg(
            deviceID,
            String.format(
                StorageEngineMessages.CURRENT_CHUNK_METADATA_MISMATCH_FMT,
                chunkMetadata,
                actualChunkTimeRange)));
  }

  public CompactionStatisticsCheckFailedException(
      IDeviceID deviceID, PageHeader pageHeader, TimeRange pageDataTimeRange) {
    super(
        getExceptionMsg(
            deviceID,
            String.format(
                StorageEngineMessages.CURRENT_PAGE_TIME_RANGE_MISMATCH_FMT,
                pageHeader,
                pageDataTimeRange)));
  }

  private static String getExceptionMsg(IDeviceID deviceID, String detail) {
    return String.format(
        StorageEngineMessages.DEVICE_TIME_RANGE_VERIFICATION_FAILED_FMT, deviceID, detail);
  }

  @Override
  @SuppressWarnings("java:S3551")
  public Throwable fillInStackTrace() {
    return this;
  }
}
