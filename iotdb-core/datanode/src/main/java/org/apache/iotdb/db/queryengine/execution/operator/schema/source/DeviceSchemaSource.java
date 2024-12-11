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

package org.apache.iotdb.db.queryengine.execution.operator.schema.source;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.runtime.SchemaExecutionException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.SchemaRegionReadPlanFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;

import static org.apache.iotdb.commons.schema.SchemaConstant.ALL_MATCH_PATTERN;

public class DeviceSchemaSource implements ISchemaSource<IDeviceSchemaInfo> {

  private final PartialPath pathPattern;
  private final PathPatternTree scope;
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
      SchemaFilter schemaFilter,
      PathPatternTree scope) {
    this.pathPattern = pathPattern;
    this.isPrefixMatch = isPrefixPath;

    this.limit = limit;
    this.offset = offset;

    this.hasSgCol = hasSgCol;
    this.schemaFilter = schemaFilter;
    this.scope = scope;
  }

  @Override
  public ISchemaReader<IDeviceSchemaInfo> getSchemaReader(ISchemaRegion schemaRegion) {
    try {
      return schemaRegion.getDeviceReader(
          SchemaRegionReadPlanFactory.getShowDevicesPlan(
              pathPattern, limit, offset, isPrefixMatch, schemaFilter, scope));
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
    builder
        .getColumnBuilder(0)
        .writeBinary(new Binary(device.getFullPath(), TSFileConfig.STRING_CHARSET));
    int templateId = device.getTemplateId();
    long ttl = DataNodeTTLCache.getInstance().getTTLInMSForTree(device.getPartialPath().getNodes());
    // TODO: make it more readable, like "30 days" or "10 hours"
    String ttlStr = ttl == Long.MAX_VALUE ? IoTDBConstant.TTL_INFINITE : String.valueOf(ttl);
    if (hasSgCol) {
      builder.getColumnBuilder(1).writeBinary(new Binary(database, TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(2)
          .writeBinary(new Binary(String.valueOf(device.isAligned()), TSFileConfig.STRING_CHARSET));
      if (templateId != SchemaConstant.NON_TEMPLATE) {
        builder
            .getColumnBuilder(3)
            .writeBinary(
                new Binary(
                    String.valueOf(
                        ClusterTemplateManager.getInstance().getTemplate(templateId).getName()),
                    TSFileConfig.STRING_CHARSET));
      } else {
        builder.getColumnBuilder(3).appendNull();
      }
      builder.getColumnBuilder(4).writeBinary(new Binary(ttlStr, TSFileConfig.STRING_CHARSET));
    } else {
      builder
          .getColumnBuilder(1)
          .writeBinary(new Binary(String.valueOf(device.isAligned()), TSFileConfig.STRING_CHARSET));
      if (templateId != SchemaConstant.NON_TEMPLATE) {
        builder
            .getColumnBuilder(2)
            .writeBinary(
                new Binary(
                    String.valueOf(
                        ClusterTemplateManager.getInstance().getTemplate(templateId).getName()),
                    TSFileConfig.STRING_CHARSET));
      } else {
        builder.getColumnBuilder(2).appendNull();
      }
      builder.getColumnBuilder(3).writeBinary(new Binary(ttlStr, TSFileConfig.STRING_CHARSET));
    }
    builder.declarePosition();
  }

  @Override
  public boolean hasSchemaStatistic(ISchemaRegion schemaRegion) {
    return (pathPattern.equals(ALL_MATCH_PATTERN)
            || pathPattern.include(
                new PartialPath((schemaRegion.getDatabaseFullPath() + ".**").split("\\."))))
        && (schemaFilter == null)
        && scope.equals(SchemaConstant.ALL_MATCH_SCOPE);
  }

  @Override
  public long getSchemaStatistic(ISchemaRegion schemaRegion) {
    return schemaRegion.getSchemaRegionStatistics().getDevicesNumber();
  }
}
