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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.memtable.MemSeriesLazyMerger;
import org.apache.iotdb.db.engine.memtable.TimeValuePairSorter;
import org.apache.iotdb.db.query.reader.MemChunkLoader;
import org.apache.iotdb.db.utils.MathUtils;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsDouble;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType.TsFloat;

//TODO: merge ReadOnlyMemChunk and WritableMemChunk and IWritableMemChunk
public class ReadOnlyMemChunk implements TimeValuePairSorter {

  private boolean initialized;
  private String measurementUid;
  private TSDataType dataType;
  private TimeValuePairSorter memSeries;
  private List<TimeValuePair> sortedTimeValuePairList;

  Map<String, String> props;
  private int floatPrecision = TSFileDescriptor.getInstance().getConfig().getFloatPrecision();

  /**
   * init by TSDataType and TimeValuePairSorter.
   */
  public ReadOnlyMemChunk(String measurementUid, TSDataType dataType, TimeValuePairSorter memSeries,
      Map<String, String> props) {
    this.measurementUid = measurementUid;
    this.dataType = dataType;
    this.memSeries = memSeries;
    this.initialized = false;
    this.props = props;
    if (props.containsKey(Encoder.MAX_POINT_NUMBER)) {
      this.floatPrecision = Integer.parseInt(props.get(Encoder.MAX_POINT_NUMBER));
    }
  }

  private void checkInitialized() {
    if (!initialized) {
      init();
    }
  }

  private void init() {
    sortedTimeValuePairList = memSeries.getSortedTimeValuePairList();
    if (!(memSeries instanceof MemSeriesLazyMerger)) {
      switch (dataType) {
        case FLOAT:
          sortedTimeValuePairList.replaceAll(x -> new TimeValuePair(x.getTimestamp(),
              new TsFloat(
                  MathUtils.roundWithGivenPrecision(x.getValue().getFloat(), floatPrecision))));
          break;
        case DOUBLE:
          sortedTimeValuePairList.replaceAll(x -> new TimeValuePair(x.getTimestamp(),
              new TsDouble(
                  MathUtils.roundWithGivenPrecision(x.getValue().getDouble(), floatPrecision))));
          break;
        default:
          break;
      }
    }
    //putBack memory
    memSeries = null;
    initialized = true;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public List<TimeValuePair> getSortedTimeValuePairList() {
    checkInitialized();
    return Collections.unmodifiableList(sortedTimeValuePairList);
  }

  @Override
  public Iterator<TimeValuePair> getIterator() {
    checkInitialized();
    return sortedTimeValuePairList.iterator();
  }

  @Override
  public boolean isEmpty() {
    checkInitialized();
    return sortedTimeValuePairList.isEmpty();
  }

  public ChunkMetaData getChunkMetaData() {
    Statistics statsByType = Statistics.getStatsByType(dataType);
    ChunkMetaData metaData = new ChunkMetaData(measurementUid, dataType, 0, statsByType);
    if (!isEmpty()) {
      for (TimeValuePair timeValuePair : getSortedTimeValuePairList()) {
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
    return metaData;
  }
}
