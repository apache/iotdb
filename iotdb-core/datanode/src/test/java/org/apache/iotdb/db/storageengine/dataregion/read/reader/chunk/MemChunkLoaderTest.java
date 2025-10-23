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
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.BinaryStatistics;
import org.apache.tsfile.file.metadata.statistics.BooleanStatistics;
import org.apache.tsfile.file.metadata.statistics.DoubleStatistics;
import org.apache.tsfile.file.metadata.statistics.FloatStatistics;
import org.apache.tsfile.file.metadata.statistics.IntegerStatistics;
import org.apache.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
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
  private static final int maxNumberOfPointsInPage = 1000;

  @Test
  public void testBooleanMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.BOOLEAN);
    Map<TVList, Integer> booleanTvListMap = buildBooleanTvListMap();
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(booleanTvListMap);
    List<TVList> booleanTvLists = new ArrayList<>(booleanTvListMap.keySet());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(TSDataType.BOOLEAN, booleanTvLists, maxNumberOfPointsInPage);
    timeValuePairIterator.setStreamingQueryMemChunk(false);
    timeValuePairIterator.nextBatch();
    Mockito.when(chunk.getMemPointIterator()).thenReturn(timeValuePairIterator);

    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);
    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    Statistics<? extends Serializable> pageStatistics = Mockito.mock(BooleanStatistics.class);
    List<Statistics<? extends Serializable>> pageStats = Collections.singletonList(pageStatistics);
    Mockito.when(chunk.getPageStatisticsList()).thenReturn(pageStats);

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics<? extends Serializable> statistics = Mockito.mock(BooleanStatistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2);

    Mockito.doReturn(statistics).when(chunkMetadata1).getStatistics();
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
    TVList tvList1 = TVList.newList(TSDataType.BOOLEAN);
    tvList1.putBoolean(1L, true);
    tvList1.putBoolean(2L, true);
    TVList tvList2 = TVList.newList(TSDataType.BOOLEAN);
    tvList2.putBoolean(1L, true);
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList1, 2);
    tvListMap.put(tvList2, 1);
    return tvListMap;
  }

  @Test
  public void testInt32MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.INT32);
    Map<TVList, Integer> int32TvListMap = buildInt32TvListMap();
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(int32TvListMap);
    List<TVList> int32TvLists = new ArrayList<>(int32TvListMap.keySet());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(TSDataType.INT32, int32TvLists, maxNumberOfPointsInPage);
    timeValuePairIterator.setStreamingQueryMemChunk(false);
    timeValuePairIterator.nextBatch();
    Mockito.when(chunk.getMemPointIterator()).thenReturn(timeValuePairIterator);

    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);
    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    Statistics<? extends Serializable> pageStatistics = Mockito.mock(IntegerStatistics.class);
    List<Statistics<? extends Serializable>> pageStats = Collections.singletonList(pageStatistics);
    Mockito.when(chunk.getPageStatisticsList()).thenReturn(pageStats);

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics<? extends Serializable> statistics = Mockito.mock(IntegerStatistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2);

    Mockito.doReturn(statistics).when(chunkMetadata1).getStatistics();
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
    TVList tvList1 = TVList.newList(TSDataType.INT32);
    tvList1.putInt(1L, 1);
    tvList1.putInt(2L, 2);
    TVList tvList2 = TVList.newList(TSDataType.INT32);
    tvList2.putInt(1L, 1);
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList1, 2);
    tvListMap.put(tvList2, 1);
    return tvListMap;
  }

  @Test
  public void testInt64MemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.INT64);
    Map<TVList, Integer> int64TvListMap = buildInt64TvListMap();
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(int64TvListMap);
    List<TVList> int64TvLists = new ArrayList<>(int64TvListMap.keySet());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(TSDataType.INT64, int64TvLists, maxNumberOfPointsInPage);
    timeValuePairIterator.setStreamingQueryMemChunk(false);
    timeValuePairIterator.nextBatch();
    Mockito.when(chunk.getMemPointIterator()).thenReturn(timeValuePairIterator);

    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);
    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    Statistics<? extends Serializable> pageStatistics = Mockito.mock(LongStatistics.class);
    List<Statistics<? extends Serializable>> pageStats = Collections.singletonList(pageStatistics);
    Mockito.when(chunk.getPageStatisticsList()).thenReturn(pageStats);

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics<? extends Serializable> statistics = Mockito.mock(LongStatistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2);

    Mockito.doReturn(statistics).when(chunkMetadata1).getStatistics();
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
    TVList tvList1 = TVList.newList(TSDataType.INT64);
    tvList1.putLong(1L, 1L);
    tvList1.putLong(2L, 2L);
    TVList tvList2 = TVList.newList(TSDataType.INT64);
    tvList2.putLong(1L, 1L);
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList1, 2);
    tvListMap.put(tvList2, 1);
    return tvListMap;
  }

  @Test
  public void testFloatMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.FLOAT);
    Map<TVList, Integer> floatTvListMap = buildFloatTvListMap();
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(floatTvListMap);
    List<TVList> floatTvLists = new ArrayList<>(floatTvListMap.keySet());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(TSDataType.FLOAT, floatTvLists, maxNumberOfPointsInPage);
    timeValuePairIterator.setStreamingQueryMemChunk(false);
    timeValuePairIterator.nextBatch();
    Mockito.when(chunk.getMemPointIterator()).thenReturn(timeValuePairIterator);

    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);
    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    Statistics<? extends Serializable> pageStatistics = Mockito.mock(FloatStatistics.class);
    List<Statistics<? extends Serializable>> pageStats = Collections.singletonList(pageStatistics);
    Mockito.when(chunk.getPageStatisticsList()).thenReturn(pageStats);

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics<? extends Serializable> statistics = Mockito.mock(FloatStatistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2);

    Mockito.doReturn(statistics).when(chunkMetadata1).getStatistics();
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
    TVList tvList1 = TVList.newList(TSDataType.FLOAT);
    tvList1.putFloat(1L, 1.1f);
    tvList1.putFloat(2L, 2.1f);
    TVList tvList2 = TVList.newList(TSDataType.FLOAT);
    tvList2.putFloat(1L, 1.1f);
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList1, 2);
    tvListMap.put(tvList2, 1);
    return tvListMap;
  }

  @Test
  public void testDoubleMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.DOUBLE);
    Map<TVList, Integer> doubleTvListMap = buildDoubleTvListMap();
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(doubleTvListMap);
    List<TVList> doubleTvLists = new ArrayList<>(doubleTvListMap.keySet());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(TSDataType.DOUBLE, doubleTvLists, maxNumberOfPointsInPage);
    timeValuePairIterator.setStreamingQueryMemChunk(false);
    timeValuePairIterator.nextBatch();
    Mockito.when(chunk.getMemPointIterator()).thenReturn(timeValuePairIterator);

    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);
    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    Statistics<? extends Serializable> pageStatistics = Mockito.mock(DoubleStatistics.class);
    List<Statistics<? extends Serializable>> pageStats = Collections.singletonList(pageStatistics);
    Mockito.when(chunk.getPageStatisticsList()).thenReturn(pageStats);

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics<? extends Serializable> statistics = Mockito.mock(DoubleStatistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2);

    Mockito.doReturn(statistics).when(chunkMetadata1).getStatistics();
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
    TVList tvList1 = TVList.newList(TSDataType.DOUBLE);
    tvList1.putDouble(1L, 1.1d);
    tvList1.putDouble(2L, 2.1d);
    TVList tvList2 = TVList.newList(TSDataType.DOUBLE);
    tvList2.putDouble(1L, 1.1d);
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList1, 2);
    tvListMap.put(tvList2, 1);
    return tvListMap;
  }

  @Test
  public void testTextMemChunkLoader() throws IOException {
    ReadOnlyMemChunk chunk = Mockito.mock(ReadOnlyMemChunk.class);
    Mockito.when(chunk.getDataType()).thenReturn(TSDataType.TEXT);
    Map<TVList, Integer> textTvListMap = buildTextTvListMap();
    Mockito.when(chunk.getTvListQueryMap()).thenReturn(textTvListMap);
    List<TVList> textTvLists = new ArrayList<>(textTvListMap.keySet());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(TSDataType.TEXT, textTvLists, maxNumberOfPointsInPage);
    timeValuePairIterator.setStreamingQueryMemChunk(false);
    timeValuePairIterator.nextBatch();
    Mockito.when(chunk.getMemPointIterator()).thenReturn(timeValuePairIterator);

    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);
    MemChunkLoader memChunkLoader = new MemChunkLoader(new QueryContext(), chunk);
    try {
      memChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    Statistics<? extends Serializable> pageStatistics = Mockito.mock(BinaryStatistics.class);
    List<Statistics<? extends Serializable>> pageStats = Collections.singletonList(pageStatistics);
    Mockito.when(chunk.getPageStatisticsList()).thenReturn(pageStats);

    ChunkMetadata chunkMetadata1 = Mockito.mock(ChunkMetadata.class);
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Mockito.when(chunk.getPointReader()).thenReturn(null);
    Statistics<? extends Serializable> statistics = Mockito.mock(BinaryStatistics.class);
    Mockito.when(statistics.getCount()).thenReturn(2);

    Mockito.doReturn(statistics).when(chunkMetadata1).getStatistics();
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
    TVList tvList1 = TVList.newList(TSDataType.TEXT);
    tvList1.putBinary(1L, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
    tvList1.putBinary(2L, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
    TVList tvList2 = TVList.newList(TSDataType.TEXT);
    tvList2.putBinary(1L, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET));
    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList1, 2);
    tvListMap.put(tvList2, 1);
    return tvListMap;
  }
}
