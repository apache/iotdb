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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.INodeSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.template.Template;

import java.util.List;
import java.util.Map;

public class SchemaSourceFactory {

  private SchemaSourceFactory() {
    // Empty constructor
  }

  // count time series
  public static ISchemaSource<ITimeSeriesSchemaInfo> getTimeSeriesSchemaCountSource(
      PartialPath pathPattern,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap,
      PathPatternTree scope) {
    return new TimeSeriesSchemaSource(
        pathPattern, isPrefixMatch, 0, 0, schemaFilter, templateMap, false, scope);
  }

  // show time series
  public static ISchemaSource<ITimeSeriesSchemaInfo> getTimeSeriesSchemaScanSource(
      PartialPath pathPattern,
      boolean isPrefixMatch,
      long limit,
      long offset,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap,
      PathPatternTree scope) {
    return new TimeSeriesSchemaSource(
        pathPattern, isPrefixMatch, limit, offset, schemaFilter, templateMap, true, scope);
  }

  // count device
  public static ISchemaSource<IDeviceSchemaInfo> getDeviceSchemaSource(
      PartialPath pathPattern, boolean isPrefixPath, PathPatternTree scope) {
    return new DeviceSchemaSource(pathPattern, isPrefixPath, 0, 0, false, null, scope);
  }

  // show device
  public static ISchemaSource<IDeviceSchemaInfo> getDeviceSchemaSource(
      PartialPath pathPattern,
      boolean isPrefixPath,
      long limit,
      long offset,
      boolean hasSgCol,
      SchemaFilter schemaFilter,
      PathPatternTree scope) {
    return new DeviceSchemaSource(
        pathPattern, isPrefixPath, limit, offset, hasSgCol, schemaFilter, scope);
  }

  // show nodes
  public static ISchemaSource<INodeSchemaInfo> getNodeSchemaSource(
      PartialPath pathPattern, int level, PathPatternTree scope) {
    return new NodeSchemaSource(pathPattern, level, scope);
  }

  public static ISchemaSource<IDeviceSchemaInfo> getPathsUsingTemplateSource(
      List<PartialPath> pathPatternList, int templateId, PathPatternTree scope) {
    return new PathsUsingTemplateSource(pathPatternList, templateId, scope);
  }

  public static ISchemaSource<ITimeSeriesSchemaInfo> getLogicalViewSchemaSource(
      PartialPath pathPattern,
      long limit,
      long offset,
      SchemaFilter schemaFilter,
      PathPatternTree scope) {
    return new LogicalViewSchemaSource(pathPattern, limit, offset, schemaFilter, scope);
  }

  public static ISchemaSource<IDeviceSchemaInfo> getTableDeviceFetchSource(
      final String database,
      final String tableName,
      final List<Object[]> deviceIdList,
      final List<ColumnHeader> columnHeaderList) {
    return new TableDeviceFetchSource(database, tableName, deviceIdList, columnHeaderList);
  }

  public static ISchemaSource<IDeviceSchemaInfo> getTableDeviceQuerySource(
      final String database,
      final String tableName,
      final List<List<SchemaFilter>> idDeterminedFilterList,
      final List<ColumnHeader> columnHeaderList,
      final DevicePredicateFilter filter) {
    return new TableDeviceQuerySource(
        database, tableName, idDeterminedFilterList, columnHeaderList, filter);
  }
}
