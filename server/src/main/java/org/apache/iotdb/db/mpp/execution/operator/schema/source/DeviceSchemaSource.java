/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.execution.operator.schema.source;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.read.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.metadata.query.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.metadata.query.reader.ISchemaReader;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.List;

import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_MATCH_PATTERN;

public class DeviceSchemaSource implements ISchemaSource<IDeviceSchemaInfo> {

  private final PartialPath pathPattern;
  private final boolean isPrefixMatch;

  private final long limit;
  private final long offset;

  private final boolean hasSgCol;

  private final SchemaFilter schemaFilter;

  DeviceSchemaSource(
      PartialPath pathPattern,
      boolean isPrefixPath,
      long limit,
      long offset,
      boolean hasSgCol,
      SchemaFilter schemaFilter) {
    this.pathPattern = pathPattern;
    this.isPrefixMatch = isPrefixPath;

    this.limit = limit;
    this.offset = offset;

    this.hasSgCol = hasSgCol;
    this.schemaFilter = schemaFilter;
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    try {
      return schemaRegion.getDeviceReader(
          SchemaRegionReadPlanFactory.getShowDevicesPlan(
              pathPattern, limit, offset, isPrefixMatch, schemaFilter));
    } catch (MetadataException e) {
      throw new SchemaExecutionException(e.getMessage(), e);
    }
  }

  @Override
  public List<ColumnHeader> getInfoQueryColumnHeaders() {
    return hasSgCol
        ? ColumnHeaderConstant.showDevicesWithSgColumnHeaders
        : ColumnHeaderConstant.showDevicesColumnHeaders;
  }

  @Override
  public void transformToTsBlockColumns(
      IDeviceSchemaInfo device, TsBlockBuilder builder, String database) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(new Binary(device.getFullPath()));
    if (hasSgCol) {
      builder.getColumnBuilder(1).writeBinary(new Binary(database));
      builder.getColumnBuilder(2).writeBinary(new Binary(String.valueOf(device.isAligned())));
    } else {
      builder.getColumnBuilder(1).writeBinary(new Binary(String.valueOf(device.isAligned())));
    }
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return pathPattern.equals(ALL_MATCH_PATTERN);
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return schemaRegion.getSchemaRegionStatistics().getDevicesNumber();
  }
}
