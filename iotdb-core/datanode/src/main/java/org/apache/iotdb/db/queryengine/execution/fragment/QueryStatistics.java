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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistic to record the count and time of load timeseries metadata, construct chunk readers and
 * page reader decompress in the query execution
 */
public class QueryStatistics {

  // statistics for count and time of load timeseriesmetadata
  public AtomicLong loadTimeSeriesMetadataDiskSeqCount = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataDiskUnSeqCount = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataMemSeqCount = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataMemUnSeqCount = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedDiskSeqCount = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedDiskUnSeqCount = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedMemSeqCount = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedMemUnSeqCount = new AtomicLong(0);

  public AtomicLong loadTimeSeriesMetadataDiskSeqTime = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataDiskUnSeqTime = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataMemSeqTime = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataMemUnSeqTime = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedDiskSeqTime = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedDiskUnSeqTime = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedMemSeqTime = new AtomicLong(0);
  public AtomicLong loadTimeSeriesMetadataAlignedMemUnSeqTime = new AtomicLong(0);

  // statistics for count and time of construct chunk readers(disk io and decompress)
  public AtomicLong constructNonAlignedChunkReadersDiskCount = new AtomicLong(0);
  public AtomicLong constructNonAlignedChunkReadersMemCount = new AtomicLong(0);
  public AtomicLong constructAlignedChunkReadersDiskCount = new AtomicLong(0);
  public AtomicLong constructAlignedChunkReadersMemCount = new AtomicLong(0);

  public AtomicLong constructNonAlignedChunkReadersDiskTime = new AtomicLong(0);
  public AtomicLong constructNonAlignedChunkReadersMemTime = new AtomicLong(0);
  public AtomicLong constructAlignedChunkReadersDiskTime = new AtomicLong(0);
  public AtomicLong constructAlignedChunkReadersMemTime = new AtomicLong(0);

  // statistics for count and time of page decode
  public AtomicLong pageReadersDecodeAlignedDiskCount = new AtomicLong(0);
  public AtomicLong pageReadersDecodeAlignedDiskTime = new AtomicLong(0);
  public AtomicLong pageReadersDecodeAlignedMemCount = new AtomicLong(0);
  public AtomicLong pageReadersDecodeAlignedMemTime = new AtomicLong(0);
  public AtomicLong pageReadersDecodeNonAlignedDiskCount = new AtomicLong(0);
  public AtomicLong pageReadersDecodeNonAlignedDiskTime = new AtomicLong(0);
  public AtomicLong pageReadersDecodeNonAlignedMemCount = new AtomicLong(0);
  public AtomicLong pageReadersDecodeNonAlignedMemTime = new AtomicLong(0);
}
