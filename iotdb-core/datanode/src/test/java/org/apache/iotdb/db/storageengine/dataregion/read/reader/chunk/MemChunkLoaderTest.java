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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk;

import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MemChunkLoaderTest {

  private static final String BINARY_STR = "ty love zm";

  @Test
  public void testBooleanMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getTsBlock()).thenReturn(buildBooleanTsBlock());
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.BOOLEAN);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.BOOLEAN));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.setFilter(null);

    memChunkLoader.close();
  }

  private TsBlock buildBooleanTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.BOOLEAN));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeBoolean(true);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeBoolean(false);
    builder.declarePosition();
    return builder.build();
  }

  @Test
  public void testInt32MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getTsBlock()).thenReturn(buildInt32TsBlock());
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.INT32);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.INT32));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.setFilter(null);

    memChunkLoader.close();
  }

  private TsBlock buildInt32TsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeInt(1);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeInt(2);
    builder.declarePosition();
    return builder.build();
  }

  @Test
  public void testInt64MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getTsBlock()).thenReturn(buildInt64TsBlock());
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.INT64);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.INT64));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.setFilter(null);

    memChunkLoader.close();
  }

  private TsBlock buildInt64TsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(1L);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeLong(2L);
    builder.declarePosition();
    return builder.build();
  }

  @Test
  public void testFloatMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getTsBlock()).thenReturn(buildFloatTsBlock());
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.FLOAT);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.FLOAT));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.setFilter(null);

    memChunkLoader.close();
  }

  private TsBlock buildFloatTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.FLOAT));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeFloat(1.1f);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeFloat(2.1f);
    builder.declarePosition();
    return builder.build();
  }

  @Test
  public void testDoubleMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getTsBlock()).thenReturn(buildDoubleTsBlock());
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.DOUBLE);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.DOUBLE));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.setFilter(null);

    memChunkLoader.close();
  }

  private TsBlock buildDoubleTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.DOUBLE));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeDouble(1.1d);
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeDouble(2.1d);
    builder.declarePosition();
    return builder.build();
  }

  @Test
  public void testTextMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

    Mockito.when(chunk.getTsBlock()).thenReturn(buildTextTsBlock());
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics statistics = Mockito.mock(Statistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(statistics);
    Mockito.when(chunkMetadata1.getDataType()).thenReturn(TSDataType.TEXT);

    MemChunkReader chunkReader =
        (MemChunkReader) memChunkLoader.getChunkReader(chunkMetadata1, null);

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemPageReader pageReader = (MemPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(Collections.singletonList(TSDataType.TEXT));

    BatchData batchData = pageReader.getAllSatisfiedPageData();
    assertEquals(2, batchData.length());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    batchData = pageReader.getAllSatisfiedPageData(false);
    assertEquals(2, batchData.length());
    assertEquals(BatchData.BatchDataType.DESC_READ, batchData.getBatchDataType());
    assertEquals(1L, batchData.getTimeByIndex(0));
    assertEquals(2L, batchData.getTimeByIndex(1));

    TsBlock tsBlock = pageReader.getAllSatisfiedData();
    assertEquals(2, tsBlock.getPositionCount());
    assertEquals(1L, tsBlock.getTimeColumn().getLong(0));
    assertEquals(2L, tsBlock.getTimeColumn().getLong(1));

    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.setFilter(null);

    memChunkLoader.close();
  }

  private TsBlock buildTextTsBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.TEXT));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeBinary(new Binary(BINARY_STR));
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).writeBinary(new Binary(BINARY_STR));
    builder.declarePosition();
    return builder.build();
  }
}
