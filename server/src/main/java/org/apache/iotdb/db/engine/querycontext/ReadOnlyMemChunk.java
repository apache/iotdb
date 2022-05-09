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
package org.apache.iotdb.db.engine.querycontext;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * ReadOnlyMemChunk is a snapshot of the working MemTable and flushing memtable in the memory used
 * for querying
 */
public class ReadOnlyMemChunk {

  private String measurementUid;

  private TSDataType dataType;

  private static final Logger logger = LoggerFactory.getLogger(ReadOnlyMemChunk.class);

  protected IChunkMetadata cachedMetaData;

  protected TsBlock tsBlock;

  protected ReadOnlyMemChunk() {}

  public ReadOnlyMemChunk(
      String measurementUid,
      TSDataType dataType,
      TSEncoding encoding,
      TVList tvList,
      Map<String, String> props,
      List<TimeRange> deletionList)
      throws IOException, QueryProcessException {
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
    if (props != null && props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      try {
        floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
      } catch (NumberFormatException e) {
        logger.warn(
            "The format of MAX_POINT_NUMBER {}  is not correct."
                + " Using default float precision.",
            props.get(Encoder.MAX_POINT_NUMBER));
      }
      if (floatPrecision < 0) {
        logger.warn(
            "The MAX_POINT_NUMBER shouldn't be less than 0." + " Using default float precision {}.",
            TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
        floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
      }
    }
    this.tsBlock = tvList.buildTsBlock(floatPrecision, encoding, deletionList);
    initChunkMetaFromTsBlock();
  }

  private void initChunkMetaFromTsBlock() throws IOException, QueryProcessException {
    Statistics statsByType = Statistics.getStatsByType(dataType);
    IChunkMetadata metaData = new ChunkMetadata(measurementUid, dataType, 0, statsByType);
    if (!isEmpty()) {
      switch (dataType) {
        case BOOLEAN:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statsByType.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getBoolean(i));
          }
          break;
        case TEXT:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statsByType.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getBinary(i));
          }
          break;
        case FLOAT:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statsByType.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getFloat(i));
          }
          break;
        case INT32:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statsByType.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getInt(i));
          }
          break;
        case INT64:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statsByType.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getLong(i));
          }
          break;
        case DOUBLE:
          for (int i = 0; i < tsBlock.getPositionCount(); i++) {
            statsByType.update(tsBlock.getTimeByIndex(i), tsBlock.getColumn(0).getDouble(i));
          }
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    }
    statsByType.setEmpty(isEmpty());
    metaData.setChunkLoader(new MemChunkLoader(this));
    metaData.setVersion(Long.MAX_VALUE);
    cachedMetaData = metaData;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean isEmpty() throws IOException {
    return tsBlock.isEmpty();
  }

  public IChunkMetadata getChunkMetaData() {
    return cachedMetaData;
  }

  public IPointReader getPointReader() {
    return tsBlock.getTsBlockSingleColumnIterator();
  }

  public TsBlock getTsBlock() {
    return tsBlock;
  }
}
