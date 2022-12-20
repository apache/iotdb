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
package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IAlignedPageReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AlignedPageReader implements IPageReader, IAlignedPageReader {

  private static final Logger logger = LoggerFactory.getLogger(AlignedPageReader.class);

  private final TimePageReader timePageReader;
  private final List<ValuePageReader> valuePageReaderList;
  private final int valueCount;
  private Filter filter;
  private boolean isModified;
  private TsBlockBuilder builder;

  private static final int MASK = 0x80;

  public AlignedPageReader(
      PageHeader timePageHeader,
      ByteBuffer timePageData,
      Decoder timeDecoder,
      List<PageHeader> valuePageHeaderList,
      List<ByteBuffer> valuePageDataList,
      List<TSDataType> valueDataTypeList,
      List<Decoder> valueDecoderList,
      Filter filter) {
    timePageReader = new TimePageReader(timePageHeader, timePageData, timeDecoder);
    isModified = timePageReader.isModified();
    valuePageReaderList = new ArrayList<>(valuePageHeaderList.size());
    for (int i = 0; i < valuePageHeaderList.size(); i++) {
      if (valuePageHeaderList.get(i) != null) {
        ValuePageReader valuePageReader =
            new ValuePageReader(
                valuePageHeaderList.get(i),
                valuePageDataList.get(i),
                valueDataTypeList.get(i),
                valueDecoderList.get(i));
        valuePageReaderList.add(valuePageReader);
        isModified = isModified || valuePageReader.isModified();
      } else {
        valuePageReaderList.add(null);
      }
    }
    this.filter = filter;
    this.valueCount = valuePageReaderList.size();
  }

  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(TSDataType.VECTOR, ascending, false);
    int timeIndex = -1;
    while (timePageReader.hasNextTime()) {
      long timestamp = timePageReader.nextTime();
      timeIndex++;
      // if all the sub sensors' value are null in current row, just discard it
      boolean isNull = true;
      Object notNullObject = null;
      TsPrimitiveType[] v = new TsPrimitiveType[valueCount];
      for (int i = 0; i < v.length; i++) {
        ValuePageReader pageReader = valuePageReaderList.get(i);
        v[i] = pageReader == null ? null : pageReader.nextValue(timestamp, timeIndex);
        if (v[i] != null) {
          isNull = false;
          notNullObject = v[i].getValue();
        }
      }
      // Currently, if it's a value filter, it will only accept AlignedPath with only one sub
      // sensor
      if (!isNull && (filter == null || filter.satisfy(timestamp, notNullObject))) {
        pageData.putVector(timestamp, v);
      }
    }
    return pageData.flip();
  }

  @Override
  public TsBlock getAllSatisfiedData() {
    builder.reset();
    int[] offsetAndLength =
        timePageReader.nextSatisfiedTimeBatch(filter, builder.getTimeColumnBuilder());

    builder.declarePositions(offsetAndLength[1]);

    // construct value columns
    for (int i = 0; i < valueCount; i++) {
      ValuePageReader pageReader = valuePageReaderList.get(i);
      pageReader.writeColumnBuilderWithNextBatch(
          offsetAndLength[0], offsetAndLength[1], builder.getColumnBuilder(i));
    }
    return builder.build();
  }

  public void setDeleteIntervalList(List<List<TimeRange>> list) {
    for (int i = 0; i < valueCount; i++) {
      if (valuePageReaderList.get(i) != null) {
        valuePageReaderList.get(i).setDeleteIntervalList(list.get(i));
      }
    }
  }

  @Override
  public Statistics getStatistics() {
    return valuePageReaderList.size() == 1 && valuePageReaderList.get(0) != null
        ? valuePageReaderList.get(0).getStatistics()
        : timePageReader.getStatistics();
  }

  @Override
  public Statistics getStatistics(int index) {
    ValuePageReader valuePageReader = valuePageReaderList.get(index);
    return valuePageReader == null ? null : valuePageReader.getStatistics();
  }

  @Override
  public Statistics getTimeStatistics() {
    return timePageReader.getStatistics();
  }

  @Override
  public void setFilter(Filter filter) {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      this.filter = new AndFilter(this.filter, filter);
    }
  }

  @Override
  public boolean isModified() {
    return isModified;
  }

  @Override
  public void initTsBlockBuilder(List<TSDataType> dataTypes) {
    builder = new TsBlockBuilder(dataTypes);
  }
}
