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

package org.apache.iotdb.tsfile.read;

import org.apache.iotdb.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.executor.TsFileExecutor;

import java.io.IOException;

public class TsFileReader implements AutoCloseable {

  private TsFileSequenceReader fileReader;
  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;
  private TsFileExecutor tsFileExecutor;

  /** Constructor, create ReadOnlyTsFile with {@link TsFileSequenceReader}. */
  public TsFileReader(TsFileSequenceReader fileReader) throws IOException {
    this.fileReader = fileReader;
    this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
    this.chunkLoader = new CachedChunkLoaderImpl(fileReader);
    tsFileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
  }

  public QueryDataSet query(QueryExpression queryExpression) throws IOException {
    return tsFileExecutor.execute(queryExpression);
  }

  public QueryDataSet query(
      QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset)
      throws IOException {
    return tsFileExecutor.execute(queryExpression, partitionStartOffset, partitionEndOffset);
  }

  @Override
  public void close() throws IOException {
    fileReader.close();
  }
}
