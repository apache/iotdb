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
package org.apache.iotdb.tsfile.read.query.timegenerator;

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;

import java.io.IOException;
import java.util.List;

public class TsFileTimeGenerator extends TimeGenerator {

  private IChunkLoader chunkLoader;
  private IMetadataQuerier metadataQuerier;

  public TsFileTimeGenerator(
      IExpression iexpression, IChunkLoader chunkLoader, IMetadataQuerier metadataQuerier)
      throws IOException {
    this.chunkLoader = chunkLoader;
    this.metadataQuerier = metadataQuerier;

    super.constructNode(iexpression);
  }

  @Override
  protected IBatchReader generateNewBatchReader(SingleSeriesExpression expression)
      throws IOException {
    List<IChunkMetadata> chunkMetadataList =
        metadataQuerier.getChunkMetaDataList(expression.getSeriesPath());
    return new FileSeriesReader(chunkLoader, chunkMetadataList, expression.getFilter());
  }

  @Override
  protected boolean isAscending() {
    return true;
  }
}
