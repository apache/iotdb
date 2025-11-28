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
import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.MemPointIterator;
import org.apache.iotdb.db.utils.datastructure.MemPointIteratorFactory;
import org.apache.iotdb.db.utils.datastructure.TVList;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.file.metadata.statistics.TimeStatistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPageReader;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MemAlignedChunkLoaderTest {

  private static final String BINARY_STR = "ty love zm";
  private static final int maxNumberOfPointsInPage = 1000;

  @Test
  public void testMemAlignedChunkLoader() throws IOException {
    AlignedReadOnlyMemChunk chunk = Mockito.mock(AlignedReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);
    QueryContext ctx = new QueryContext();
    MemAlignedChunkLoader memAlignedChunkLoader = new MemAlignedChunkLoader(ctx, chunk);

    try {
      memAlignedChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    // Mock getTimeStatisticsList & getValuesStatisticsList
    List<Statistics<? extends Serializable>> timeStatitsticsList = new ArrayList<>();
    Statistics<? extends Serializable> timeStatistics = Mockito.mock(TimeStatistics.class);
    Mockito.when(timeStatistics.getCount()).thenReturn(2);
    timeStatitsticsList.add(timeStatistics);
    Mockito.when(chunk.getTimeStatisticsList()).thenReturn(timeStatitsticsList);

    List<Statistics<? extends Serializable>[]> valuesStatitsticsList = new ArrayList<>();
    Statistics<? extends Serializable>[] valuesStatistics = new Statistics[6];
    Statistics<? extends Serializable> statistics1 = Mockito.mock(Statistics.class);
    Mockito.when(statistics1.hasNullValue(2)).thenReturn(true);
    valuesStatistics[0] = statistics1;
    Statistics<? extends Serializable> statistics2 = Mockito.mock(Statistics.class);
    Mockito.when(statistics2.hasNullValue(2)).thenReturn(true);
    valuesStatistics[1] = statistics2;
    Statistics<? extends Serializable> statistics3 = Mockito.mock(Statistics.class);
    Mockito.when(statistics3.hasNullValue(2)).thenReturn(true);
    valuesStatistics[2] = statistics3;
    Statistics<? extends Serializable> statistics4 = Mockito.mock(Statistics.class);
    Mockito.when(statistics4.hasNullValue(2)).thenReturn(true);
    valuesStatistics[3] = statistics4;
    Statistics<? extends Serializable> statistics5 = Mockito.mock(Statistics.class);
    Mockito.when(statistics5.hasNullValue(2)).thenReturn(true);
    valuesStatistics[4] = statistics5;
    Statistics<? extends Serializable> statistics6 = Mockito.mock(Statistics.class);
    Mockito.when(statistics6.hasNullValue(2)).thenReturn(true);
    valuesStatistics[5] = statistics6;
    valuesStatitsticsList.add(valuesStatistics);
    Mockito.when(chunk.getValuesStatisticsList()).thenReturn(valuesStatitsticsList);

    // Mock AlignedReadOnlyMemChunk Getter
    List<TSDataType> dataTypes = buildTsDataTypes();
    Mockito.when(chunk.getDataTypes()).thenReturn(dataTypes);
    Mockito.when(chunk.getTimeColumnDeletion()).thenReturn(null);
    Mockito.when(chunk.getValueColumnsDeletionList()).thenReturn(null);
    Mockito.when(chunk.getContext()).thenReturn(ctx);

    Map<TVList, Integer> alignedTvListMap = buildAlignedTvListMap();
    Mockito.when(chunk.getAligendTvListQueryMap()).thenReturn(alignedTvListMap);
    List<AlignedTVList> alignedTvLists =
        alignedTvListMap.keySet().stream().map(x -> (AlignedTVList) x).collect(Collectors.toList());
    MemPointIterator timeValuePairIterator =
        MemPointIteratorFactory.create(
            dataTypes, null, alignedTvLists, false, maxNumberOfPointsInPage);
    timeValuePairIterator.setStreamingQueryMemChunk(false);
    timeValuePairIterator.nextBatch();
    Mockito.when(chunk.getMemPointIterator()).thenReturn(timeValuePairIterator);

    AlignedChunkMetadata chunkMetadata1 = Mockito.mock(AlignedChunkMetadata.class);
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);

    MemAlignedChunkReader chunkReader =
        (MemAlignedChunkReader) memAlignedChunkLoader.getChunkReader(chunkMetadata1, null);

    try {
      chunkReader.hasNextSatisfiedPage();
      fail();
    } catch (IOException e) {
      assertEquals("mem chunk reader does not support this method", e.getMessage());
    }

    try {
      chunkReader.nextPageData();
      fail();
    } catch (IOException e) {
      assertEquals("mem chunk reader does not support this method", e.getMessage());
    }

    List<IPageReader> pageReaderList = chunkReader.loadPageReaderList();
    assertEquals(1, pageReaderList.size());

    MemAlignedPageReader pageReader = (MemAlignedPageReader) pageReaderList.get(0);

    pageReader.initTsBlockBuilder(buildTsDataTypes());

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

    assertEquals(timeStatistics, pageReader.getStatistics());
    assertEquals(statistics1, pageReader.getMeasurementStatistics(0).orElse(null));
    assertEquals(timeStatistics, pageReader.getTimeStatistics());
    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.addRecordFilter(null);

    memAlignedChunkLoader.close();
  }

  private List<TSDataType> buildTsDataTypes() {
    return Arrays.asList(
        TSDataType.BOOLEAN,
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.DOUBLE,
        TSDataType.TEXT);
  }

  private Map<TVList, Integer> buildAlignedTvListMap() {
    List<TSDataType> dataTypes = buildTsDataTypes();
    AlignedTVList tvList1 = AlignedTVList.newAlignedList(dataTypes);
    tvList1.putAlignedValue(
        1L,
        new Object[] {
          true, 1, 2L, 1.2f, null, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET)
        });
    tvList1.putAlignedValue(2L, new Object[] {null, null, null, null, 3.14d, null});
    AlignedTVList tvList2 = AlignedTVList.newAlignedList(dataTypes);
    tvList2.putAlignedValue(
        1L,
        new Object[] {
          true, 1, 1L, 1.1f, null, new Binary(BINARY_STR, TSFileConfig.STRING_CHARSET)
        });

    Map<TVList, Integer> tvListMap = new LinkedHashMap<>();
    tvListMap.put(tvList1, 2);
    tvListMap.put(tvList2, 1);
    return tvListMap;
  }
}
