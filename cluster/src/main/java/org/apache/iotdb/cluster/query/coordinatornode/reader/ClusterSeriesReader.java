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
package org.apache.iotdb.cluster.query.coordinatornode.reader;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;

public class ClusterSeriesReader implements IPointReader {

  private IBatchReader rpcBatchReader;
  private String fullPath;
  private TSDataType dataType;

  public ClusterSeriesReader(IBatchReader rpcBatchReader, String fullPath,
      TSDataType dataType) {
    this.rpcBatchReader = rpcBatchReader;
    this.fullPath = fullPath;
    this.dataType = dataType;
  }

  @Override
  public TimeValuePair current() throws IOException {
    return null;
  }

  @Override
  public boolean hasNext() throws IOException {
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  public IBatchReader getRpcBatchReader() {
    return rpcBatchReader;
  }

  public void setRpcBatchReader(IBatchReader rpcBatchReader) {
    this.rpcBatchReader = rpcBatchReader;
  }

  public String getFullPath() {
    return fullPath;
  }

  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }
}
