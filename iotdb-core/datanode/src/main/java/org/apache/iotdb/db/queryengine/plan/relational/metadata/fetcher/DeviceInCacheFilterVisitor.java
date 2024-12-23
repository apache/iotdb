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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.StringValueFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.AttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.common.conf.TSFileConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeviceInCacheFilterVisitor extends SchemaFilterVisitor<DeviceEntry> {

  private final Map<String, Integer> attributeIndexMap = new HashMap<>();

  DeviceInCacheFilterVisitor(final List<String> attributeColumns) {
    for (int i = 0; i < attributeColumns.size(); i++) {
      attributeIndexMap.put(attributeColumns.get(i), i);
    }
  }

  @Override
  protected Boolean visitNode(final SchemaFilter filter, final DeviceEntry deviceEntry) {
    throw new UnsupportedOperationException(
        "The schema filter type " + filter.getSchemaFilterType() + " is not supported");
  }

  @Override
  public Boolean visitIdFilter(final IdFilter filter, final DeviceEntry deviceEntry) {
    // The first segment is "tableName", skip it
    final int index = filter.getIndex() + 1;
    return filter
        .getChild()
        .accept(StringValueFilterVisitor.getInstance(), (String) deviceEntry.getNthSegment(index));
  }

  @Override
  public Boolean visitAttributeFilter(final AttributeFilter filter, final DeviceEntry deviceEntry) {
    return filter
        .getChild()
        .accept(
            StringValueFilterVisitor.getInstance(),
            deviceEntry
                .getAttributeColumnValues()
                .get(attributeIndexMap.get(filter.getKey()))
                .getStringValue(TSFileConfig.STRING_CHARSET));
  }
}
