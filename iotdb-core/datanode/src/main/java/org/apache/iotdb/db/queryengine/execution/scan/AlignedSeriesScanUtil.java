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

package org.apache.iotdb.db.queryengine.execution.scan;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.AlignedDescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.AlignedPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.DescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.PriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.IMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class AlignedSeriesScanUtil extends SeriesScanUtil {

  private final List<TSDataType> dataTypes;

  // only used for limit and offset push down optimizer, if we select all columns from aligned
  // device, we can use statistics to skip.
  // it's only exact while using limit & offset push down
  private final boolean queryAllSensors;

  public AlignedSeriesScanUtil(
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context) {
    this(seriesPath, scanOrder, scanOptions, context, false, null);
  }

  public AlignedSeriesScanUtil(
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context,
      boolean queryAllSensors,
      List<TSDataType> givenDataTypes) {
    super(seriesPath, scanOrder, scanOptions, context);
    dataTypes =
        givenDataTypes != null
            ? givenDataTypes
            : ((AlignedPath) seriesPath)
                .getSchemaList().stream()
                    .map(IMeasurementSchema::getType)
                    .collect(Collectors.toList());
    isAligned = true;
    this.queryAllSensors = queryAllSensors;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // methods which need override
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  protected PriorityMergeReader getPriorityMergeReader() {
    return new AlignedPriorityMergeReader();
  }

  @Override
  protected DescPriorityMergeReader getDescPriorityMergeReader() {
    return new AlignedDescPriorityMergeReader();
  }

  @Override
  protected AlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource, PartialPath seriesPath, QueryContext context) throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource, (AlignedPath) seriesPath, context, scanOptions.getGlobalTimeFilter());
  }

  @Override
  public List<TSDataType> getTsDataTypeList() {
    return dataTypes;
  }

  @Override
  protected IPointReader getPointReader(TsBlock tsBlock) {
    return tsBlock.getTsBlockAlignedRowIterator();
  }

  @Override
  protected boolean timeAllSelected(IMetadata metadata) {
    if (queryAllSensors || metadata.getMeasurementCount() == 0) {
      return true;
    }
    return metadata.timeAllSelected();
  }
}
