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
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * BatchReader without time generator for cluster which is used in query node.
 */
public class ClusterBatchReaderWithoutTimeGenerator extends IClusterBatchReader {

  /**
   * Data type
   */
  private TSDataType dataType;

  /**
   * Point reader
   */
  private IPointReader reader;

  public ClusterBatchReaderWithoutTimeGenerator(
      TSDataType dataType, IPointReader reader) {
    this.dataType = dataType;
    this.reader = reader;
  }

  @Override
  public boolean hasNext() throws IOException {
    return reader.hasNext();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    BatchData batchData = new BatchData(dataType, true);
    for (int i = 0; i < ClusterConstant.BATCH_READ_SIZE; i++) {
      if (hasNext()) {
        TimeValuePair pair = reader.next();
        batchData.putTime(pair.getTimestamp());
        batchData.putAnObject(pair.getValue().getValue());
      } else {
        break;
      }
    }
    return batchData;
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }

  @Override
  public BatchData nextBatch(List<Long> batchTime) throws IOException {
    throw new IOException(
        "nextBatch(List<Long> batchTime) in ClusterBatchReaderWithoutTimeGenerator is an empty method.");
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public IPointReader getReader() {
    return reader;
  }
}
