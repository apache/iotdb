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

package org.apache.iotdb.commons.schema.filter.impl;

import org.apache.iotdb.commons.path.ExtendedPartialPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.IdFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;

public class DeviceFilterUtil {

  private DeviceFilterUtil() {
    // do nothing
  }

  // if the element in idDeterminedFilterList isEmpty, the corresponding pattern is
  // root.db.table.*.*..
  // e.g. input (db, table[c1, c2], [[]]), return [root.db.table.*.*]
  public static List<PartialPath> convertToDevicePattern(
      final String database,
      final String tableName,
      final int idColumnNum,
      final List<List<SchemaFilter>> idDeterminedFilterList) {
    final List<PartialPath> pathList = new ArrayList<>();
    final int length = idColumnNum + 3;
    for (final List<SchemaFilter> idFilterList : idDeterminedFilterList) {
      final String[] nodes = new String[length];
      Arrays.fill(nodes, ONE_LEVEL_PATH_WILDCARD);
      nodes[0] = PATH_ROOT;
      nodes[1] = database;
      nodes[2] = tableName;
      final ExtendedPartialPath partialPath = new ExtendedPartialPath(nodes);
      for (final SchemaFilter schemaFilter : idFilterList) {
        if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.ID)) {
          final int index = ((IdFilter) schemaFilter).getIndex() + 3;
          final SchemaFilter childFilter = ((IdFilter) schemaFilter).getChild();
          if (childFilter.getSchemaFilterType().equals(SchemaFilterType.PRECISE)) {
            // If there is a precise filter, other filters on the same id are processed and thus
            // not exist here
            nodes[index] = ((PreciseFilter) childFilter).getValue();
          } else {
            partialPath.addMatchFunction(
                index,
                node ->
                    Boolean.TRUE.equals(
                        childFilter.accept(StringValueFilterVisitor.getInstance(), node)));
          }
        } else {
          throw new IllegalStateException("Input single filter must be DeviceIdFilter");
        }
      }
      pathList.add(partialPath);
    }
    return pathList;
  }
}
