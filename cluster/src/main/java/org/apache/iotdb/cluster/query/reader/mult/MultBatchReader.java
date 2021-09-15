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
package org.apache.iotdb.cluster.query.reader.mult;

import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;
import java.util.Map;

public class MultBatchReader implements IMultBatchReader {

  private Map<String, IBatchReader> pathBatchReaders;

  public MultBatchReader(Map<String, IBatchReader> pathBatchReaders) {
    this.pathBatchReaders = pathBatchReaders;
  }

  /**
   * reader has next batch data
   *
   * @return true if only one reader has next batch data, otherwise false
   * @throws IOException
   */
  @Override
  public boolean hasNextBatch() throws IOException {
    for (IBatchReader reader : pathBatchReaders.values()) {
      if (reader != null && reader.hasNextBatch()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasNextBatch(String fullPath) throws IOException {
    IBatchReader reader = pathBatchReaders.get(fullPath);
    return reader != null && reader.hasNextBatch();
  }

  @Override
  public BatchData nextBatch(String fullPath) throws IOException {
    return pathBatchReaders.get(fullPath).nextBatch();
  }

  @Override
  public BatchData nextBatch() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * close in query resource
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {}
}
