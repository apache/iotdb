/**
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
package org.apache.iotdb.cluster.query.dataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.timegenerator.ClusterTimeGenerator;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

public class ClusterDataSetWithTimeGenerator extends QueryDataSet {

  private ClusterRpcSingleQueryManager queryManager;
  private ClusterTimeGenerator timeGenerator;
  private List<EngineReaderByTimeStamp> readers;

  /**
   * Cached batch timestamp
   */
  private Iterator<Long> cachedBatchTimestamp;
  private boolean hasCachedRowRecord;
  private RowRecord cachedRowRecord;

  /**
   * constructor of EngineDataSetWithTimeGenerator.
   *
   * @param paths paths in List structure
   * @param dataTypes time series data type
   * @param timeGenerator EngineTimeGenerator object
   * @param readers readers in List(EngineReaderByTimeStamp) structure
   */
  public ClusterDataSetWithTimeGenerator(List<Path> paths, List<TSDataType> dataTypes,
      ClusterTimeGenerator timeGenerator, List<EngineReaderByTimeStamp> readers,
      ClusterRpcSingleQueryManager queryManager) {
    super(paths, dataTypes);
    this.timeGenerator = timeGenerator;
    this.readers = readers;
    this.queryManager = queryManager;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedRowRecord) {
      return true;
    }
    return cacheRowRecord();
  }

  @Override
  public RowRecord next() throws IOException {
    if (!hasCachedRowRecord && !cacheRowRecord()) {
      return null;
    }
    hasCachedRowRecord = false;
    return cachedRowRecord;
  }

  /**
   * Cache row record
   *
   * @return if there has next row record.
   */
  private boolean cacheRowRecord() throws IOException {
    while (hasNextTimestamp()) {
      boolean hasField = false;
      long timestamp = cachedBatchTimestamp.next();
      RowRecord rowRecord = new RowRecord(timestamp);
      for (int i = 0; i < readers.size(); i++) {
        EngineReaderByTimeStamp reader = readers.get(i);
        Object value = reader.getValueInTimestamp(timestamp);
        if (value == null) {
          rowRecord.addField(new Field(null));
        } else {
          hasField = true;
          rowRecord.addField(getField(value, dataTypes.get(i)));
        }
      }
      if (hasField) {
        hasCachedRowRecord = true;
        cachedRowRecord = rowRecord;
        break;
      }
    }
    return hasCachedRowRecord;
  }

  /**
   * Check if it has next valid timestamp
   */
  private boolean hasNextTimestamp() throws IOException {
    if(cachedBatchTimestamp == null || !cachedBatchTimestamp.hasNext()) {
      List<Long> batchTimestamp = new ArrayList<>();
      for (int i = 0; i < ClusterConstant.BATCH_READ_SIZE; i++) {
        if (timeGenerator.hasNext()) {
          batchTimestamp.add(timeGenerator.next());
        } else {
          break;
        }
      }
      if (!batchTimestamp.isEmpty()) {
        cachedBatchTimestamp = batchTimestamp.iterator();
        try {
          queryManager.fetchBatchDataByTimestamp(batchTimestamp);
        } catch (RaftConnectionException e) {
          throw new IOException(e);
        }
      }
    }
    if(cachedBatchTimestamp != null && cachedBatchTimestamp.hasNext()){
      return true;
    }
    return false;
  }
}
