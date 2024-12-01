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

import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MemChunkLoaderTest {

  private static final String BINARY_STR = "ty love zm";

  @Test
  public void testBooleanMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.BOOLEAN);
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(buildBooleanTvListMap());
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

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
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private Map<TVList, Integer> buildBooleanTvListMap() {
    TVList tvList = TVList.newList(TSDataType.BOOLEAN);
    if (tvList != null) {
      tvList.putBoolean(1L, true);
      tvList.putBoolean(2L, true);
    }
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList, 2);
    return tvListMap;
  }

  @Test
  public void testInt32MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.INT32);
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(buildInt32TvListMap());
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

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
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private Map<TVList, Integer> buildInt32TvListMap() {
    TVList tvList = TVList.newList(TSDataType.INT32);
    if (tvList != null) {
      tvList.putInt(1L, 1);
      tvList.putInt(2L, 2);
    }
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList, 2);
    return tvListMap;
  }

  @Test
  public void testInt64MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.INT64);
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(buildInt64TvListMap());
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

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
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private Map<TVList, Integer> buildInt64TvListMap() {
    TVList tvList = TVList.newList(TSDataType.INT64);
    if (tvList != null) {
      tvList.putLong(1L, 1L);
      tvList.putLong(2L, 2L);
    }
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList, 2);
    return tvListMap;
  }

  @Test
  public void testFloatMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.FLOAT);
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(buildFloatTvListMap());
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

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
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private Map<TVList, Integer> buildFloatTvListMap() {
    TVList tvList = TVList.newList(TSDataType.FLOAT);
    if (tvList != null) {
      tvList.putFloat(1L, 1.1f);
      tvList.putFloat(2L, 2.1f);
    }
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList, 2);
    return tvListMap;
  }

  @Test
  public void testDoubleMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.DOUBLE);
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(buildDoubleTvListMap());
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

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
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private Map<TVList, Integer> buildDoubleTvListMap() {
    TVList tvList = TVList.newList(TSDataType.DOUBLE);
    if (tvList != null) {
      tvList.putDouble(1L, 1.1d);
      tvList.putDouble(2L, 2.1d);
    }
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList, 2);
    return tvListMap;
  }

  @Test
  public void testTextMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.TEXT);
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(buildTextTvListMap());
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);

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
    pageReader.addRecordFilter(null);

    memChunkLoader.close();
  }

  private Map<TVList, Integer> buildTextTvListMap() {
    TVList tvList = TVList.newList(TSDataType.TEXT);
    if (tvList != null) {
      tvList.putBinary(1L, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
      tvList.putBinary(2L, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
    }
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList, 2);
    return tvListMap;
  }
}
