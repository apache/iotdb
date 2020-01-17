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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.version.VersionPair;
import org.apache.iotdb.db.query.reader.MemChunkLoader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;

//TODO: merge ReadOnlyMemChunk and WritableMemChunk and IWritableMemChunk
public class ReadOnlyMemChunk {

  private String measurementUid;
  private TSDataType dataType;

  Map<String, String> props;
  private int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();
  private ChunkMetaData cachedMetaData;

  private PriorityMergeReader mergeReader;
  private PriorityMergeReader chunkedReader;

  /**
   * init by TSDataType and TimeValuePairSorter.
   */
  public ReadOnlyMemChunk(String measurementUid, TSDataType dataType,
      List<TVList> memSeries, Map<String, String> props) throws IOException {
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    this.props = props;
    if (props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      this.floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
    }
    mergeReader = new PriorityMergeReader(floatPrecision);
    chunkedReader = new PriorityMergeReader(floatPrecision);
    for (TVList pair : memSeries) {
      mergeReader.addReader(pair.getIterator(), pair.getVersion());
      chunkedReader.addReader(pair.getIterator(), pair.getVersion());
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public boolean isEmpty() {
    return !mergeReader.hasNextTimeValuePair();
  }

  public ChunkMetaData getChunkMetaData() throws IOException {
    if (cachedMetaData != null) {
      return cachedMetaData;
    }
    Statistics statsByType = Statistics.getStatsByType(dataType);
    ChunkMetaData metaData = new ChunkMetaData(measurementUid, dataType, 0, statsByType);
    if (!isEmpty()) {
      while (chunkedReader.hasNextTimeValuePair()) {
        TimeValuePair timeValuePair = chunkedReader.nextTimeValuePair();
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
            throw new RuntimeException("Unsupported data types");
        }
      }
    }
    statsByType.setEmpty(isEmpty());
    metaData.setChunkLoader(new MemChunkLoader(this));
    metaData.setVersion(Long.MAX_VALUE);
    cachedMetaData = metaData;
    return metaData;
  }

  public IPointReader getIterator() {
    return mergeReader;
  }
}
