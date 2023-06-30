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

import org.apache.iotdb.db.storageengine.dataregion.memtable.AlignedReadOnlyMemChunk;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
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
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.tsfile.read.reader.series.PaginationController.UNLIMITED_PAGINATION_CONTROLLER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class MemAlignedChunkLoaderTest {

  private static final String BINARY_STR = "ty love zm";

  @Test
  public void testMemAlignedChunkLoader() throws IOException {
    AlignedReadOnlyMemChunk chunk = Mockito.mock(AlignedReadOnlyMemChunk.class);
    ChunkMetadata chunkMetadata = Mockito.mock(ChunkMetadata.class);

    MemAlignedChunkLoader memAlignedChunkLoader = new MemAlignedChunkLoader(chunk);
    try {
      memAlignedChunkLoader.loadChunk(chunkMetadata);
      fail();
    } catch (UnsupportedOperationException e) {
      assertNull(e.getMessage());
    }

    AlignedChunkMetadata chunkMetadata1 = Mockito.mock(AlignedChunkMetadata.class);

    Mockito.when(chunk.getTsBlock()).thenReturn(buildTsBlock());
    Mockito.when(chunk.getChunkMetaData()).thenReturn(chunkMetadata1);
    Statistics statistics1 = Mockito.mock(Statistics.class);
    Mockito.when(statistics1.hasNullValue(2)).thenReturn(true);
    Statistics statistics2 = Mockito.mock(Statistics.class);
    Mockito.when(statistics2.hasNullValue(2)).thenReturn(true);
    Statistics statistics3 = Mockito.mock(Statistics.class);
    Mockito.when(statistics3.hasNullValue(2)).thenReturn(true);
    Statistics statistics4 = Mockito.mock(Statistics.class);
    Mockito.when(statistics4.hasNullValue(2)).thenReturn(true);
    Statistics statistics5 = Mockito.mock(Statistics.class);
    Mockito.when(statistics5.hasNullValue(2)).thenReturn(true);
    Statistics statistics6 = Mockito.mock(Statistics.class);
    Mockito.when(statistics6.hasNullValue(2)).thenReturn(true);

    Statistics timeStatistics = Mockito.mock(Statistics.class);
    Mockito.when(timeStatistics.getCount()).thenReturn(2L);

    Mockito.when(chunkMetadata1.getStatistics()).thenReturn(timeStatistics);
    Mockito.when(chunkMetadata1.getTimeStatistics()).thenReturn(timeStatistics);
    Mockito.when(chunkMetadata1.getStatistics(0)).thenReturn(statistics1);
    Mockito.when(chunkMetadata1.getValueStatisticsList())
        .thenReturn(
            Arrays.asList(
                statistics1, statistics2, statistics3, statistics4, statistics5, statistics6));

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

    pageReader.initTsBlockBuilder(
        Arrays.asList(
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT));

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
    assertEquals(statistics1, pageReader.getStatistics(0));
    assertEquals(timeStatistics, pageReader.getTimeStatistics());
    assertFalse(pageReader.isModified());
    pageReader.setLimitOffset(UNLIMITED_PAGINATION_CONTROLLER);
    pageReader.setFilter(null);

    memAlignedChunkLoader.close();
  }

  private TsBlock buildTsBlock() {
    TsBlockBuilder builder =
        new TsBlockBuilder(
            Arrays.asList(
                TSDataType.BOOLEAN,
                TSDataType.INT32,
                TSDataType.INT64,
                TSDataType.FLOAT,
                TSDataType.DOUBLE,
                TSDataType.TEXT));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeBoolean(true);
    builder.getColumnBuilder(1).writeInt(1);
    builder.getColumnBuilder(2).writeLong(1L);
    builder.getColumnBuilder(3).writeFloat(1.1f);
    builder.getColumnBuilder(4).appendNull();
    builder.getColumnBuilder(5).writeBinary(new Binary(BINARY_STR));
    builder.declarePosition();
    builder.getTimeColumnBuilder().writeLong(2L);
    builder.getColumnBuilder(0).appendNull();
    builder.getColumnBuilder(1).appendNull();
    builder.getColumnBuilder(2).appendNull();
    builder.getColumnBuilder(3).appendNull();
    builder.getColumnBuilder(4).writeDouble(3.14d);
    builder.getColumnBuilder(5).appendNull();
    builder.declarePosition();
    return builder.build();
  }
}
