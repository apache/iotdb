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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistic to record the count and time of load timeseries metadata, construct chunk readers and
 * page reader decompress in the query execution
 */
public class QueryStatistics {

  // statistics for count and time of load timeseriesmetadata
  private final AtomicLong loadTimeSeriesMetadataDiskSeqCount = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataDiskUnSeqCount = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataMemSeqCount = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataMemUnSeqCount = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedDiskSeqCount = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedDiskUnSeqCount = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedMemSeqCount = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedMemUnSeqCount = new AtomicLong(0);

  private final AtomicLong loadTimeSeriesMetadataDiskSeqTime = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataDiskUnSeqTime = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataMemSeqTime = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataMemUnSeqTime = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedDiskSeqTime = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedDiskUnSeqTime = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedMemSeqTime = new AtomicLong(0);
  private final AtomicLong loadTimeSeriesMetadataAlignedMemUnSeqTime = new AtomicLong(0);

  // statistics for count and time of construct chunk readers(disk io and decompress)
  private final AtomicLong constructNonAlignedChunkReadersDiskCount = new AtomicLong(0);
  private final AtomicLong constructNonAlignedChunkReadersMemCount = new AtomicLong(0);
  private final AtomicLong constructAlignedChunkReadersDiskCount = new AtomicLong(0);
  private final AtomicLong constructAlignedChunkReadersMemCount = new AtomicLong(0);

  private final AtomicLong constructNonAlignedChunkReadersDiskTime = new AtomicLong(0);
  private final AtomicLong constructNonAlignedChunkReadersMemTime = new AtomicLong(0);
  private final AtomicLong constructAlignedChunkReadersDiskTime = new AtomicLong(0);
  private final AtomicLong constructAlignedChunkReadersMemTime = new AtomicLong(0);

  // statistics for count and time of page decode
  private final AtomicLong pageReadersDecodeAlignedDiskCount = new AtomicLong(0);
  private final AtomicLong pageReadersDecodeAlignedDiskTime = new AtomicLong(0);
  private final AtomicLong pageReadersDecodeAlignedMemCount = new AtomicLong(0);
  private final AtomicLong pageReadersDecodeAlignedMemTime = new AtomicLong(0);
  private final AtomicLong pageReadersDecodeNonAlignedDiskCount = new AtomicLong(0);
  private final AtomicLong pageReadersDecodeNonAlignedDiskTime = new AtomicLong(0);
  private final AtomicLong pageReadersDecodeNonAlignedMemCount = new AtomicLong(0);
  private final AtomicLong pageReadersDecodeNonAlignedMemTime = new AtomicLong(0);

  private final AtomicLong nonAlignedTimeSeriesMetadataModificationCount = new AtomicLong(0);
  private final AtomicLong nonAlignedTimeSeriesMetadataModificationTime = new AtomicLong(0);
  private final AtomicLong alignedTimeSeriesMetadataModificationCount = new AtomicLong(0);
  private final AtomicLong alignedTimeSeriesMetadataModificationTime = new AtomicLong(0);

  // statistics for count and time of page decode
  private final AtomicLong pageReaderMaxUsedMemorySize = new AtomicLong(0);

  public AtomicLong getLoadTimeSeriesMetadataDiskSeqCount() {
    return loadTimeSeriesMetadataDiskSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataDiskUnSeqCount() {
    return loadTimeSeriesMetadataDiskUnSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataMemSeqCount() {
    return loadTimeSeriesMetadataMemSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataMemUnSeqCount() {
    return loadTimeSeriesMetadataMemUnSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedDiskSeqCount() {
    return loadTimeSeriesMetadataAlignedDiskSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedDiskUnSeqCount() {
    return loadTimeSeriesMetadataAlignedDiskUnSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedMemSeqCount() {
    return loadTimeSeriesMetadataAlignedMemSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedMemUnSeqCount() {
    return loadTimeSeriesMetadataAlignedMemUnSeqCount;
  }

  public AtomicLong getLoadTimeSeriesMetadataDiskSeqTime() {
    return loadTimeSeriesMetadataDiskSeqTime;
  }

  public AtomicLong getLoadTimeSeriesMetadataDiskUnSeqTime() {
    return loadTimeSeriesMetadataDiskUnSeqTime;
  }

  public AtomicLong getLoadTimeSeriesMetadataMemSeqTime() {
    return loadTimeSeriesMetadataMemSeqTime;
  }

  public AtomicLong getLoadTimeSeriesMetadataMemUnSeqTime() {
    return loadTimeSeriesMetadataMemUnSeqTime;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedDiskSeqTime() {
    return loadTimeSeriesMetadataAlignedDiskSeqTime;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedDiskUnSeqTime() {
    return loadTimeSeriesMetadataAlignedDiskUnSeqTime;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedMemSeqTime() {
    return loadTimeSeriesMetadataAlignedMemSeqTime;
  }

  public AtomicLong getLoadTimeSeriesMetadataAlignedMemUnSeqTime() {
    return loadTimeSeriesMetadataAlignedMemUnSeqTime;
  }

  public AtomicLong getConstructNonAlignedChunkReadersDiskCount() {
    return constructNonAlignedChunkReadersDiskCount;
  }

  public AtomicLong getConstructNonAlignedChunkReadersMemCount() {
    return constructNonAlignedChunkReadersMemCount;
  }

  public AtomicLong getConstructAlignedChunkReadersDiskCount() {
    return constructAlignedChunkReadersDiskCount;
  }

  public AtomicLong getConstructAlignedChunkReadersMemCount() {
    return constructAlignedChunkReadersMemCount;
  }

  public AtomicLong getConstructNonAlignedChunkReadersDiskTime() {
    return constructNonAlignedChunkReadersDiskTime;
  }

  public AtomicLong getConstructNonAlignedChunkReadersMemTime() {
    return constructNonAlignedChunkReadersMemTime;
  }

  public AtomicLong getConstructAlignedChunkReadersDiskTime() {
    return constructAlignedChunkReadersDiskTime;
  }

  public AtomicLong getConstructAlignedChunkReadersMemTime() {
    return constructAlignedChunkReadersMemTime;
  }

  public AtomicLong getPageReadersDecodeAlignedDiskCount() {
    return pageReadersDecodeAlignedDiskCount;
  }

  public AtomicLong getPageReadersDecodeAlignedDiskTime() {
    return pageReadersDecodeAlignedDiskTime;
  }

  public AtomicLong getPageReadersDecodeAlignedMemCount() {
    return pageReadersDecodeAlignedMemCount;
  }

  public AtomicLong getPageReadersDecodeAlignedMemTime() {
    return pageReadersDecodeAlignedMemTime;
  }

  public AtomicLong getPageReadersDecodeNonAlignedDiskCount() {
    return pageReadersDecodeNonAlignedDiskCount;
  }

  public AtomicLong getPageReadersDecodeNonAlignedDiskTime() {
    return pageReadersDecodeNonAlignedDiskTime;
  }

  public AtomicLong getPageReadersDecodeNonAlignedMemCount() {
    return pageReadersDecodeNonAlignedMemCount;
  }

  public AtomicLong getPageReadersDecodeNonAlignedMemTime() {
    return pageReadersDecodeNonAlignedMemTime;
  }

  public AtomicLong getNonAlignedTimeSeriesMetadataModificationCount() {
    return nonAlignedTimeSeriesMetadataModificationCount;
  }

  public AtomicLong getNonAlignedTimeSeriesMetadataModificationTime() {
    return nonAlignedTimeSeriesMetadataModificationTime;
  }

  public AtomicLong getAlignedTimeSeriesMetadataModificationCount() {
    return alignedTimeSeriesMetadataModificationCount;
  }

  public AtomicLong getAlignedTimeSeriesMetadataModificationTime() {
    return alignedTimeSeriesMetadataModificationTime;
  }

  public AtomicLong getPageReaderMaxUsedMemorySize() {
    return pageReaderMaxUsedMemorySize;
  }

  public TQueryStatistics toThrift() {
    return new TQueryStatistics(
        loadTimeSeriesMetadataDiskSeqCount.get(),
        loadTimeSeriesMetadataDiskUnSeqCount.get(),
        loadTimeSeriesMetadataMemSeqCount.get(),
        loadTimeSeriesMetadataMemUnSeqCount.get(),
        loadTimeSeriesMetadataAlignedDiskSeqCount.get(),
        loadTimeSeriesMetadataAlignedDiskUnSeqCount.get(),
        loadTimeSeriesMetadataAlignedMemSeqCount.get(),
        loadTimeSeriesMetadataAlignedMemUnSeqCount.get(),
        loadTimeSeriesMetadataDiskSeqTime.get(),
        loadTimeSeriesMetadataDiskUnSeqTime.get(),
        loadTimeSeriesMetadataMemSeqTime.get(),
        loadTimeSeriesMetadataMemUnSeqTime.get(),
        loadTimeSeriesMetadataAlignedDiskSeqTime.get(),
        loadTimeSeriesMetadataAlignedDiskUnSeqTime.get(),
        loadTimeSeriesMetadataAlignedMemSeqTime.get(),
        loadTimeSeriesMetadataAlignedMemUnSeqTime.get(),
        constructNonAlignedChunkReadersDiskCount.get(),
        constructNonAlignedChunkReadersMemCount.get(),
        constructAlignedChunkReadersDiskCount.get(),
        constructAlignedChunkReadersMemCount.get(),
        constructNonAlignedChunkReadersDiskTime.get(),
        constructNonAlignedChunkReadersMemTime.get(),
        constructAlignedChunkReadersDiskTime.get(),
        constructAlignedChunkReadersMemTime.get(),
        pageReadersDecodeAlignedDiskCount.get(),
        pageReadersDecodeAlignedDiskTime.get(),
        pageReadersDecodeAlignedMemCount.get(),
        pageReadersDecodeAlignedMemTime.get(),
        pageReadersDecodeNonAlignedDiskCount.get(),
        pageReadersDecodeNonAlignedDiskTime.get(),
        pageReadersDecodeNonAlignedMemCount.get(),
        pageReadersDecodeNonAlignedMemTime.get(),
        pageReaderMaxUsedMemorySize.get(),
        alignedTimeSeriesMetadataModificationCount.get(),
        alignedTimeSeriesMetadataModificationTime.get(),
        nonAlignedTimeSeriesMetadataModificationCount.get(),
        nonAlignedTimeSeriesMetadataModificationTime.get());
  }
}
