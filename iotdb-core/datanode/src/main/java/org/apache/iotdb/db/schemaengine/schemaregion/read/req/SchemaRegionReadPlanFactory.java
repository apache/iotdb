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
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterFactory;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowDevicesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowNodesPlanImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl.ShowTimeSeriesPlanImpl;
import org.apache.iotdb.db.schemaengine.template.Template;

import java.util.Collections;
import java.util.Map;

public class SchemaRegionReadPlanFactory {

  private SchemaRegionReadPlanFactory() {}

  @TestOnly
  public static IShowDevicesPlan getShowDevicesPlan(PartialPath path) {
    return new ShowDevicesPlanImpl(path, 0, 0, false, -1, null);
  }

  @TestOnly
  public static IShowDevicesPlan getShowDevicesPlan(PartialPath path, boolean isPrefixMatch) {
    return new ShowDevicesPlanImpl(path, 0, 0, isPrefixMatch, -1, null);
  }

  public static IShowDevicesPlan getShowDevicesPlan(
      PartialPath path, long limit, long offset, boolean isPrefixMatch, SchemaFilter schemaFilter) {
    return new ShowDevicesPlanImpl(path, limit, offset, isPrefixMatch, -1, schemaFilter);
  }

  public static IShowDevicesPlan getShowDevicesPlan(
      PartialPath path, int limit, int offset, boolean isPrefixMatch, int templateId) {
    return new ShowDevicesPlanImpl(path, limit, offset, isPrefixMatch, templateId, null);
  }

  @TestOnly
  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(PartialPath path) {
    return new ShowTimeSeriesPlanImpl(path, Collections.emptyMap(), 0, 0, false, null, false);
  }

  @TestOnly
  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path, Map<Integer, Template> relatedTemplate) {
    return new ShowTimeSeriesPlanImpl(path, relatedTemplate, 0, 0, false, null, false);
  }

  @TestOnly
  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path, boolean isContains, String key, String value) {
    return new ShowTimeSeriesPlanImpl(
        path,
        Collections.emptyMap(),
        0,
        0,
        false,
        SchemaFilterFactory.createTagFilter(key, value, isContains),
        false);
  }

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path,
      Map<Integer, Template> relatedTemplate,
      long limit,
      long offset,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter,
      boolean needViewDetail) {
    return new ShowTimeSeriesPlanImpl(
        path, relatedTemplate, limit, offset, isPrefixMatch, schemaFilter, needViewDetail);
  }

  public static IShowNodesPlan getShowNodesPlan(PartialPath path) {
    return new ShowNodesPlanImpl(path, -1, false);
  }

  public static IShowNodesPlan getShowNodesPlan(
      PartialPath path, int level, boolean isPrefixMatch) {
    return new ShowNodesPlanImpl(path, level, isPrefixMatch);
  }
}
