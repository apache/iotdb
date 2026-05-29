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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;

import java.io.IOException;
import java.util.List;

public class ExternalTsFileSeriesScanUtil extends AlignedSeriesScanUtil {

  private final ExternalTsFileMetadataLoader metadataLoader;

  public ExternalTsFileSeriesScanUtil(
      AlignedFullPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context,
      boolean queryAllSensors,
      List<TSDataType> givenDataTypes,
      ExternalTsFileMetadataLoader metadataLoader) {
    super(seriesPath, scanOrder, scanOptions, context, queryAllSensors, givenDataTypes);
    this.metadataLoader = metadataLoader;
  }

  public ExternalTsFileSeriesScanUtil(
      AlignedFullPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context,
      boolean queryAllSensors,
      List<TSDataType> givenDataTypes,
      MultiTsFileResourceIterator resourceIterator) {
    this(
        seriesPath,
        scanOrder,
        scanOptions,
        context,
        queryAllSensors,
        givenDataTypes,
        resourceIterator::loadTimeSeriesMetadata);
  }

  @Override
  protected AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource, boolean isSeq) throws IOException {
    return metadataLoader.loadTimeSeriesMetadata(resource, (AlignedFullPath) seriesPath);
  }

  @FunctionalInterface
  public interface ExternalTsFileMetadataLoader {
    AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(
        TsFileResource resource, AlignedFullPath alignedFullPath) throws IOException;
  }
}
