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
import org.apache.iotdb.tsfile.file.metadata.VectorChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ReadOnlyMemChunk is a snapshot of the working MemTable and flushing memtable in the memory used
 * for querying
 */
public class ReadOnlyMemChunk {

  // deletion list for this chunk
  private final List<TimeRange> deletionList;

  private String measurementUid;
  private TSDataType dataType;
  private TSEncoding encoding;

  private static final Logger logger = LoggerFactory.getLogger(ReadOnlyMemChunk.class);

  private int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();

  private IChunkMetadata cachedMetaData;

  private TVList chunkData;

  private IPointReader chunkPointReader;

  private int chunkDataSize;

  public ReadOnlyMemChunk(
      String measurementUid,
      TSDataType dataType,
      TSEncoding encoding,
      TVList tvList,
      Map<String, String> props,
      int size,
      List<TimeRange> deletionList)
      throws IOException, QueryProcessException {
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    this.encoding = encoding;
    if (props != null && props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      try {
        this.floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
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

    this.chunkData = tvList;
    this.chunkDataSize = size;
    this.deletionList = deletionList;

    this.chunkPointReader =
        tvList.getIterator(floatPrecision, encoding, chunkDataSize, deletionList);
    initChunkMeta();
  }

  private void initChunkMeta() throws IOException, QueryProcessException {
    Statistics statsByType = Statistics.getStatsByType(dataType);
    IChunkMetadata metaData = new ChunkMetadata(measurementUid, dataType, 0, statsByType);
    if (!isEmpty()) {
      IPointReader iterator =
          chunkData.getIterator(floatPrecision, encoding, chunkDataSize, deletionList);
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        switch (dataType) {
          case BOOLEAN:
            statsByType.update(timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
            break;
          case TEXT:
            statsByType.update(timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
            break;
          case FLOAT:
            statsByType.update(timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
            break;
          case INT32:
            statsByType.update(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
            break;
          case INT64:
            statsByType.update(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
            break;
          case DOUBLE:
            statsByType.update(timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
            break;
          default:
            throw new QueryProcessException("Unsupported data type:" + dataType);
        }
      }
    }
    statsByType.setEmpty(isEmpty());
    metaData.setChunkLoader(new MemChunkLoader(this));
    metaData.setVersion(Long.MAX_VALUE);
    cachedMetaData = metaData;
  }

  /**
   * The constructor for VECTOR type.
   *
   * @param schema VectorMeasurementSchema
   * @param tvList VectorTvList
   * @param size The Number of Chunk data points
   * @param deletionList The timeRange of deletionList
   */
  public ReadOnlyMemChunk(
      IMeasurementSchema schema, TVList tvList, int size, List<TimeRange> deletionList)
      throws IOException, QueryProcessException {
    this.measurementUid = schema.getMeasurementId();
    this.dataType = schema.getType();

    this.chunkData = tvList;
    this.chunkDataSize = size;
    this.deletionList = deletionList;

    this.chunkPointReader =
        tvList.getIterator(floatPrecision, encoding, chunkDataSize, deletionList);
    initVectorChunkMeta(schema);
  }

  @SuppressWarnings("squid:S3776") // high Cognitive Complexity
  private void initVectorChunkMeta(IMeasurementSchema schema)
      throws IOException, QueryProcessException {
    Statistics timeStatistics = Statistics.getStatsByType(TSDataType.VECTOR);
    IChunkMetadata timeChunkMetadata =
        new ChunkMetadata(measurementUid, TSDataType.VECTOR, 0, timeStatistics);
    List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
    Statistics[] valueStatistics = new Statistics[schema.getSubMeasurementsTSDataTypeList().size()];
    for (int i = 0; i < schema.getSubMeasurementsTSDataTypeList().size(); i++) {
      valueStatistics[i] =
          Statistics.getStatsByType(schema.getSubMeasurementsTSDataTypeList().get(i));
      IChunkMetadata valueChunkMetadata =
          new ChunkMetadata(
              schema.getSubMeasurementsList().get(i),
              schema.getSubMeasurementsTSDataTypeList().get(i),
              0,
              valueStatistics[i]);
      valueChunkMetadataList.add(valueChunkMetadata);
    }
    if (!isEmpty()) {
      IPointReader iterator =
          chunkData.getIterator(floatPrecision, encoding, chunkDataSize, deletionList);
      while (iterator.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = iterator.nextTimeValuePair();
        timeStatistics.update(timeValuePair.getTimestamp());
        if (schema.getSubMeasurementsTSDataTypeList().size() == 1) {
          updateValueStatisticsForSingleColumn(schema, valueStatistics, timeValuePair);
        } else {
          updateValueStatistics(schema, valueStatistics, timeValuePair);
        }
      }
    }
    timeStatistics.setEmpty(isEmpty());
    for (Statistics valueStatistic : valueStatistics) {
      valueStatistic.setEmpty(isEmpty());
    }
    IChunkMetadata vectorChunkMetadata =
        new VectorChunkMetadata(timeChunkMetadata, valueChunkMetadataList);
    vectorChunkMetadata.setChunkLoader(new MemChunkLoader(this));
    vectorChunkMetadata.setVersion(Long.MAX_VALUE);
    cachedMetaData = vectorChunkMetadata;
  }

  // When query one measurement in a Vector, the timeValuePair is not a vector type
  private void updateValueStatisticsForSingleColumn(
      IMeasurementSchema schema, Statistics[] valueStatistics, TimeValuePair timeValuePair)
      throws QueryProcessException {
    switch (schema.getSubMeasurementsTSDataTypeList().get(0)) {
      case BOOLEAN:
        valueStatistics[0].update(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getBoolean());
        break;
      case TEXT:
        valueStatistics[0].update(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getBinary());
        break;
      case FLOAT:
        valueStatistics[0].update(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getFloat());
        break;
      case INT32:
        valueStatistics[0].update(timeValuePair.getTimestamp(), timeValuePair.getValue().getInt());
        break;
      case INT64:
        valueStatistics[0].update(timeValuePair.getTimestamp(), timeValuePair.getValue().getLong());
        break;
      case DOUBLE:
        valueStatistics[0].update(
            timeValuePair.getTimestamp(), timeValuePair.getValue().getDouble());
        break;
      default:
        throw new QueryProcessException("Unsupported data type:" + dataType);
    }
  }

  private void updateValueStatistics(
      IMeasurementSchema schema, Statistics[] valueStatistics, TimeValuePair timeValuePair)
      throws QueryProcessException {
    for (int i = 0; i < schema.getSubMeasurementsTSDataTypeList().size(); i++) {
      if (timeValuePair.getValue().getVector()[i] == null) {
        continue;
      }
      switch (schema.getSubMeasurementsTSDataTypeList().get(i)) {
        case BOOLEAN:
          valueStatistics[i].update(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getVector()[i].getBoolean());
          break;
        case TEXT:
          valueStatistics[i].update(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getVector()[i].getBinary());
          break;
        case FLOAT:
          valueStatistics[i].update(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getVector()[i].getFloat());
          break;
        case INT32:
          valueStatistics[i].update(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getVector()[i].getInt());
          break;
        case INT64:
          valueStatistics[i].update(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getVector()[i].getLong());
          break;
        case DOUBLE:
          valueStatistics[i].update(
              timeValuePair.getTimestamp(), timeValuePair.getValue().getVector()[i].getDouble());
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataType);
      }
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean isEmpty() throws IOException {
    return !chunkPointReader.hasNextTimeValuePair();
  }

  public IChunkMetadata getChunkMetaData() {
    return cachedMetaData;
  }

  public IPointReader getPointReader() {
    chunkPointReader = chunkData.getIterator(floatPrecision, encoding, chunkDataSize, deletionList);
    return chunkPointReader;
  }

  public String getMeasurementUid() {
    return measurementUid;
  }
}
