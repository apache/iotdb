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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl.read;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowNodesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.plan.schemafilter.SchemaFilter;
import org.apache.iotdb.db.mpp.plan.schemafilter.impl.TagFilter;

import java.util.Collections;
import java.util.Map;

public class SchemaRegionReadPlanFactory {

  private SchemaRegionReadPlanFactory() {}

  public static IShowDevicesPlan getShowDevicesPlan(PartialPath path) {
    return new ShowDevicesPlanImpl(path, 0, 0, false, -1);
  }

  public static IShowDevicesPlan getShowDevicesPlan(PartialPath path, boolean isPrefixMatch) {
    return new ShowDevicesPlanImpl(path, 0, 0, isPrefixMatch, -1);
  }

  public static IShowDevicesPlan getShowDevicesPlan(
      PartialPath path, long limit, long offset, boolean isPrefixMatch) {
    return new ShowDevicesPlanImpl(path, limit, offset, isPrefixMatch, -1);
  }

  public static IShowDevicesPlan getShowDevicesPlan(
      PartialPath path, int limit, int offset, boolean isPrefixMatch, int templateId) {
    return new ShowDevicesPlanImpl(path, limit, offset, isPrefixMatch, templateId);
  }

  @TestOnly
  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(PartialPath path) {
    return new ShowTimeSeriesPlanImpl(path, Collections.emptyMap(), 0, 0, false, null);
  }

  @TestOnly
  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path, Map<Integer, Template> relatedTemplate) {
    return new ShowTimeSeriesPlanImpl(path, relatedTemplate, 0, 0, false, null);
  }

  @TestOnly
  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path, boolean isContains, String key, String value) {
    return new ShowTimeSeriesPlanImpl(
        path, Collections.emptyMap(), 0, 0, false, new TagFilter(key, value, isContains));
  }

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path,
      Map<Integer, Template> relatedTemplate,
      long limit,
      long offset,
      boolean isPrefixMatch,
      SchemaFilter schemaFilter) {
    return new ShowTimeSeriesPlanImpl(
        path, relatedTemplate, limit, offset, isPrefixMatch, schemaFilter);
  }

  public static IShowNodesPlan getShowNodesPlan(PartialPath path) {
    return new ShowNodesPlanImpl(path, -1, false);
  }

  public static IShowNodesPlan getShowNodesPlan(
      PartialPath path, int level, boolean isPrefixMatch) {
    return new ShowNodesPlanImpl(path, level, isPrefixMatch);
  }
}
