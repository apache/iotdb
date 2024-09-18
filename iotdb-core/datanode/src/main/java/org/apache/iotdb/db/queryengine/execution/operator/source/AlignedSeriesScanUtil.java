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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.AlignedDescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.AlignedPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.DescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.PriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.IMetadata;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class AlignedSeriesScanUtil extends SeriesScanUtil {

  private final List<TSDataType> dataTypes;

  // only used for limit and offset push down optimizer, if we select all columns from aligned
  // device, we can use statistics to skip.
  // it's only exact while using limit & offset push down
  // for table scan, it should always be true
  private final boolean queryAllSensors;

  // for table model, it will be false
  // for tree model, it will be true
  private final boolean ignoreAllNullRows;

  public AlignedSeriesScanUtil(
      AlignedFullPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context) {
    this(seriesPath, scanOrder, scanOptions, context, false, null, true);
  }

  public AlignedSeriesScanUtil(
      AlignedFullPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context,
      boolean queryAllSensors,
      List<TSDataType> givenDataTypes,
      boolean ignoreAllNullRows) {
    super(seriesPath, scanOrder, scanOptions, context);
    isAligned = true;
    this.dataTypes =
        givenDataTypes != null
            ? givenDataTypes
            : seriesPath.getSchemaList().stream()
                .map(IMeasurementSchema::getType)
                .collect(Collectors.toList());
    this.queryAllSensors = queryAllSensors;
    this.ignoreAllNullRows = ignoreAllNullRows;
  }

  @Override
  protected PriorityMergeReader getPriorityMergeReader() {
    return new AlignedPriorityMergeReader();
  }

  @Override
  protected DescPriorityMergeReader getDescPriorityMergeReader() {
    return new AlignedDescPriorityMergeReader();
  }

  @Override
  protected AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource, boolean isSeq) throws IOException {
    return FileLoaderUtils.loadAlignedTimeSeriesMetadata(
        resource,
        (AlignedFullPath) seriesPath,
        context,
        scanOptions.getGlobalTimeFilter(),
        isSeq,
        ignoreAllNullRows);
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
