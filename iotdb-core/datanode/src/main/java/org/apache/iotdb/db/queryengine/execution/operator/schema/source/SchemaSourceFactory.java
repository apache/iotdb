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

  public static ISchemaSource<ITimeSeriesSchemaInfo> getTimeSeriesSchemaCountSource(
      PartialPath pathPattern,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap) {
    return new TimeSeriesSchemaSource(
        pathPattern, isPrefixMatch, 0, 0, schemaFilter, templateMap, false);
  }

  public static ISchemaSource<ITimeSeriesSchemaInfo> getTimeSeriesSchemaScanSource(
      PartialPath pathPattern,
      boolean isPrefixMatch,
      long limit,
      long offset,
      SchemaFilter schemaFilter,
      Map<Integer, Template> templateMap) {
    return new TimeSeriesSchemaSource(
        pathPattern, isPrefixMatch, limit, offset, schemaFilter, templateMap, true);
  }

  public static ISchemaSource<IDeviceSchemaInfo> getDeviceSchemaSource(
      PartialPath pathPattern, boolean isPrefixPath) {
    return new DeviceSchemaSource(pathPattern, isPrefixPath, 0, 0, false, null);
  }

  public static ISchemaSource<IDeviceSchemaInfo> getDeviceSchemaSource(
      PartialPath pathPattern,
      boolean isPrefixPath,
      long limit,
      long offset,
      boolean hasSgCol,
      SchemaFilter schemaFilter) {
    return new DeviceSchemaSource(pathPattern, isPrefixPath, limit, offset, hasSgCol, schemaFilter);
  }

  public static ISchemaSource<INodeSchemaInfo> getNodeSchemaSource(
      PartialPath pathPattern, int level) {
    return new NodeSchemaSource(pathPattern, level);
  }

  public static ISchemaSource<IDeviceSchemaInfo> getPathsUsingTemplateSource(
      List<PartialPath> pathPatternList, int templateId) {
    return new PathsUsingTemplateSource(pathPatternList, templateId);
  }

  public static ISchemaSource<ITimeSeriesSchemaInfo> getLogicalViewSchemaSource(
      PartialPath pathPattern, long limit, long offset, SchemaFilter schemaFilter) {
    return new LogicalViewSchemaSource(pathPattern, limit, offset, schemaFilter);
  }
}
