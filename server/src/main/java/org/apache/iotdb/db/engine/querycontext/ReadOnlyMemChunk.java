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

import java.io.IOException;
import java.util.Map;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.reader.chunk.MemChunkLoader;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

public class ReadOnlyMemChunk {

  private String measurementUid;
  private TSDataType dataType;
  private TSEncoding encoding;

  private long version;

  private int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();

  private ChunkMetadata cachedMetaData;

  private TVList chunkData;

  private IPointReader chunkPointReader;

  public ReadOnlyMemChunk(String measurementUid, TSDataType dataType, TSEncoding encoding,
      TVList tvList, Map<String, String> props, long version)
      throws IOException, QueryProcessException {
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    this.encoding = encoding;
    this.version = version;
    if (props != null && props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      this.floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
    }
    tvList.sort();
    this.chunkData = tvList;
    this.chunkPointReader = tvList.getIterator(floatPrecision, encoding);
    initChunkMeta();
  }

  private void initChunkMeta() throws IOException, QueryProcessException {
    Statistics statsByType = Statistics.getStatsByType(dataType);
    ChunkMetadata metaData = new ChunkMetadata(measurementUid, dataType, 0, statsByType);
    if (!isEmpty()) {
      IPointReader iterator = chunkData.getIterator(floatPrecision, encoding);
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

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean isEmpty() throws IOException {
    return !chunkPointReader.hasNextTimeValuePair();
  }

  public ChunkMetadata getChunkMetaData() {
    return cachedMetaData;
  }

  public IPointReader getPointReader() {
    chunkPointReader = chunkData.getIterator(floatPrecision, encoding);
    return chunkPointReader;
  }

  public long getVersion() {
    return version;
  }

  public String getMeasurementUid() {
    return measurementUid;
  }
}
