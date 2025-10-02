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

package org.apache.iotdb.db.schemaengine.schemaregion.read.req;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowDevicesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowNodesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowTimeSeriesPlanImpl;

import java.util.Map;

public class SchemaRegionReadPlanFactory {

  private SchemaRegionReadPlanFactory() {}

  public static IShowDevicesPlan getShowDevicesPlan(
      PartialPath path,
      long limit,
      long offset,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter,
      PathPatternTree scope) {
    return new ShowDevicesPlanImpl(path, limit, offset, isPrefixMatch, -1, schemaFilter, scope);
  }

  public static IShowDevicesPlan getShowDevicesPlan(
      PartialPath path,
      int limit,
      int offset,
      boolean isPrefixMatch,
      int templateId,
      PathPatternTree scope) {
    return new ShowDevicesPlanImpl(path, limit, offset, isPrefixMatch, templateId, null, scope);
  }

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path,
      Map<Integer, Template> relatedTemplate,
      long limit,
      long offset,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter,
      boolean needViewDetail,
      PathPatternTree scope) {
    return new ShowTimeSeriesPlanImpl(
        path, relatedTemplate, limit, offset, isPrefixMatch, schemaFilter, needViewDetail, scope);
  }

  public static IShowNodesPlan getShowNodesPlan(PartialPath path, PathPatternTree scope) {
    return new ShowNodesPlanImpl(path, -1, false, scope);
  }

  public static IShowNodesPlan getShowNodesPlan(
      PartialPath path, int level, boolean isPrefixMatch, PathPatternTree scope) {
    return new ShowNodesPlanImpl(path, level, isPrefixMatch, scope);
  }
}
