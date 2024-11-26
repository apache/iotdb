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

package org.apache.tsfile.read.reader.chunk;

import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.page.AbstractAlignedPageReader;
import org.apache.tsfile.read.reader.page.LazyLoadPageData;
import org.apache.tsfile.read.reader.page.TablePageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

// difference with AlignedChunkReader is that TableChunkReader works for TableScan and keep all null
// rows
public class TableChunkReader extends AbstractAlignedChunkReader {

  public TableChunkReader(
      Chunk timeChunk, List<Chunk> valueChunkList, long readStopTime, Filter queryFilter)
      throws IOException {
    super(timeChunk, valueChunkList, readStopTime, queryFilter);
  }

  public TableChunkReader(Chunk timeChunk, List<Chunk> valueChunkList, Filter queryFilter)
      throws IOException {
    this(timeChunk, valueChunkList, Long.MIN_VALUE, queryFilter);
  }

  @Override
  boolean needSkipForSinglePageChunk(boolean isAllNull, PageHeader timePageHeader) {
    return isEarlierThanReadStopTime(timePageHeader);
  }

  @Override
  boolean needSkipForMultiPageChunk(boolean isAllNull, PageHeader timePageHeader) {
    return isEarlierThanReadStopTime(timePageHeader) || pageCanSkip(timePageHeader);
  }

  @Override
  boolean canSkip(boolean isAllNull, PageHeader timePageHeader) {
    return pageDeleted(timePageHeader, timeDeleteIntervalList);
  }

  @Override
  AbstractAlignedPageReader constructPageReader(
      PageHeader timePageHeader,
      ByteBuffer timePageData,
      Decoder timeDecoder,
      List<PageHeader> valuePageHeaderList,
      LazyLoadPageData[] lazyLoadPageDataArray,
      List<TSDataType> valueDataTypeList,
      List<Decoder> valueDecoderList,
      Filter queryFilter,
      List<List<TimeRange>> valueDeleteIntervalsList) {
    TablePageReader alignedPageReader =
        new TablePageReader(
            timePageHeader,
            timePageData,
            timeDecoder,
            valuePageHeaderList,
            lazyLoadPageDataArray,
            valueDataTypeList,
            valueDecoderList,
            queryFilter);
    alignedPageReader.setDeleteIntervalList(timeDeleteIntervalList, valueDeleteIntervalsList);
    return alignedPageReader;
  }
}
