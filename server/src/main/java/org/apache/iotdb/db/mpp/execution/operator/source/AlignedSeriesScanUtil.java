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
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.universal.AlignedDescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.AlignedPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.DescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlignedSeriesScanUtil extends SeriesScanUtil {

  private final List<TSDataType> dataTypes;

  public AlignedSeriesScanUtil(
      PartialPath seriesPath,
      Set<String> allSensors,
      FragmentInstanceContext context,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    super(seriesPath, allSensors, TSDataType.VECTOR, context, timeFilter, valueFilter, ascending);
    dataTypes =
        ((AlignedPath) seriesPath)
            .getSchemaList().stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
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
  protected AlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource, (AlignedPath) seriesPath, context, filter);
  }

  @Override
  protected List<TSDataType> getTsDataTypeList() {
    return dataTypes;
  }

  @Override
  protected IPointReader getPointReader(TsBlock tsBlock) {
    return tsBlock.getTsBlockAlignedRowIterator();
  }
}
