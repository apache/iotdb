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
package org.apache.iotdb.cluster.query.reader.querynode;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * BatchReader by timestamp for cluster which is used in query node.
 */
public class ClusterBatchReaderByTimestamp extends AbstractClusterBatchReader {

  /**
   * Reader
   */
  private EngineReaderByTimeStamp readerByTimeStamp;

  /**
   * Data type
   */
  private TSDataType dataType;

  public ClusterBatchReaderByTimestamp(
      EngineReaderByTimeStamp readerByTimeStamp,
      TSDataType dataType) {
    this.readerByTimeStamp = readerByTimeStamp;
    this.dataType = dataType;
  }

  @Override
  public boolean hasNext() throws IOException {
    return readerByTimeStamp.hasNext();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    throw new UnsupportedOperationException(
        "nextBatch() in ClusterBatchReaderByTimestamp is an empty method.");
  }


  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  public BatchData nextBatch(List<Long> batchTime) throws IOException {
    BatchData batchData = new BatchData(dataType, true);
    for(long time: batchTime){
      Object value = readerByTimeStamp.getValueInTimestamp(time);
      if(value != null){
        batchData.putTime(time);
        batchData.putAnObject(value);
      }
    }
    return batchData;
  }

  public EngineReaderByTimeStamp getReaderByTimeStamp() {
    return readerByTimeStamp;
  }

  public TSDataType getDataType() {
    return dataType;
  }
}
