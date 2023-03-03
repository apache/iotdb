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

package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.query.reader.materializer.TsFileResourceMaterializer;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class FileScanUtil {

  private final Map<PartialPath, List<Aggregator>> pathToAggregatorsMap;

  private final TsFileResourceMaterializer fileResourceMaterializer;

  private final TreeSet<ChunkMetadata> chunkMetadataList =
      new TreeSet<>(
          Comparator.comparingLong(ChunkMetadata::getVersion)
              .thenComparingLong(ChunkMetadata::getOffsetOfChunkHeader));

  public FileScanUtil(
      Map<PartialPath, List<Aggregator>> pathToAggregatorsMap, QueryDataSource dataSource) {
    this.pathToAggregatorsMap = pathToAggregatorsMap;
    this.fileResourceMaterializer = new TsFileResourceMaterializer(dataSource);
  }

  public boolean hasNextFile() {
    return fileResourceMaterializer.hasNext();
  }

  public void consume() {
    TsFileResource nextFile = fileResourceMaterializer.next();

    List<ITimeSeriesMetadata> timeSeriesMetadata;
  }
}
