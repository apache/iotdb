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
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowDevicesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowNodesPlan;
import org.apache.iotdb.db.metadata.plan.schemaregion.read.IShowTimeSeriesPlan;
import org.apache.iotdb.db.metadata.template.Template;

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

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(PartialPath path) {
    return new ShowTimeSeriesPlanImpl(path, Collections.emptyMap(), false, null, null, 0, 0, false);
  }

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(PartialPath path, int limit, int offset) {
    return new ShowTimeSeriesPlanImpl(
        path, Collections.emptyMap(), false, null, null, limit, offset, false);
  }

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path, Map<Integer, Template> relatedTemplate) {
    return new ShowTimeSeriesPlanImpl(path, relatedTemplate, false, null, null, 0, 0, false);
  }

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path, boolean isContains, String key, String value) {
    return new ShowTimeSeriesPlanImpl(
        path, Collections.emptyMap(), isContains, key, value, 0, 0, false);
  }

  public static IShowTimeSeriesPlan getShowTimeSeriesPlan(
      PartialPath path,
      Map<Integer, Template> relatedTemplate,
      boolean isContains,
      String key,
      String value,
      long limit,
      long offset,
      boolean isPrefixMatch) {
    return new ShowTimeSeriesPlanImpl(
        path, relatedTemplate, isContains, key, value, limit, offset, isPrefixMatch);
  }

  public static IShowNodesPlan getShowNodesPlan(PartialPath path) {
    return new ShowNodesPlanImpl(path, -1, false);
  }

  public static IShowNodesPlan getShowNodesPlan(
      PartialPath path, int level, boolean isPrefixMatch) {
    return new ShowNodesPlanImpl(path, level, isPrefixMatch);
  }
}
