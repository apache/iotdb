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
package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Read one Chunk and cache it into a LRUCache, only used in tsfile module. */
public class CachedChunkLoaderImpl implements IChunkLoader {

  private static final int DEFAULT_CHUNK_CACHE_SIZE = 1000;
  private TsFileSequenceReader reader;
  private LRUCache<ChunkCacheKey, Chunk> chunkCache;

  public CachedChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
    this(fileSequenceReader, DEFAULT_CHUNK_CACHE_SIZE);
  }

  /**
   * constructor of ChunkLoaderImpl.
   *
   * @param fileSequenceReader file sequence reader
   * @param cacheSize cache size
   */
  public CachedChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {

    this.reader = fileSequenceReader;

    chunkCache =
        new LRUCache<ChunkCacheKey, Chunk>(cacheSize) {

          @Override
          protected Chunk loadObjectByKey(ChunkCacheKey chunkCacheKey) throws IOException {
            return reader.readMemChunk(chunkCacheKey);
          }
        };
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) throws IOException {
    Chunk chunk = chunkCache.get(new ChunkCacheKey(chunkMetaData));
    return new Chunk(
        chunk.getHeader(),
        chunk.getData().duplicate(),
        chunkMetaData.getDeleteIntervalList(),
        chunkMetaData.getStatistics());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public IChunkReader getChunkReader(IChunkMetadata chunkMetaData, Filter timeFilter)
      throws IOException {
    Chunk chunk = chunkCache.get(new ChunkCacheKey((ChunkMetadata) chunkMetaData));
    return new ChunkReader(
        new Chunk(
            chunk.getHeader(),
            chunk.getData().duplicate(),
            chunkMetaData.getDeleteIntervalList(),
            chunkMetaData.getStatistics()),
        timeFilter);
  }

  public static class ChunkCacheKey {

    private final Long offsetOfChunkHeader;
    private final String measurementUid;
    private final List<TimeRange> deleteIntervalList;
    private final Statistics<? extends Serializable> statistics;

    public ChunkCacheKey(ChunkMetadata chunkMetadata) {
      offsetOfChunkHeader = chunkMetadata.getOffsetOfChunkHeader();
      measurementUid = chunkMetadata.getMeasurementUid();
      deleteIntervalList = chunkMetadata.getDeleteIntervalList();
      statistics = chunkMetadata.getStatistics();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ChunkCacheKey that = (ChunkCacheKey) o;
      return Objects.equals(offsetOfChunkHeader, that.offsetOfChunkHeader)
          && Objects.equals(measurementUid, that.measurementUid)
          && deleteIntervalList == that.deleteIntervalList
          && statistics == that.statistics;
    }

    @Override
    public int hashCode() {
      return Objects.hash(offsetOfChunkHeader, measurementUid, deleteIntervalList, statistics);
    }

    public Long getOffsetOfChunkHeader() {
      return offsetOfChunkHeader;
    }

    public String getMeasurementUid() {
      return measurementUid;
    }

    public List<TimeRange> getDeleteIntervalList() {
      return deleteIntervalList;
    }

    public Statistics<? extends Serializable> getStatistics() {
      return statistics;
    }
  }
}
