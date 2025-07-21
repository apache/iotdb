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
import org.apache.iotdb.commons.schema.filter.impl.singlechild.TagFilter;
import org.apache.iotdb.commons.schema.filter.impl.values.PreciseFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class DeviceFilterUtil {

  private DeviceFilterUtil() {
    // do nothing
  }

  // if the element in idDeterminedFilterList isEmpty, the corresponding pattern is
  // root.db.table.*.*..
  // e.g. input (db, table[c1, c2], [[]]), return [root.db.table.*.*]
  public static List<PartialPath> convertToDevicePattern(
      final String[] prefix,
      final int tagColumnNum,
      final List<List<SchemaFilter>> tagDeterminedFilterList,
      final boolean isRestrict) {
    final List<PartialPath> pathList = new ArrayList<>();
    final int length = tagColumnNum + prefix.length;
    for (final List<SchemaFilter> tagFilterList : tagDeterminedFilterList) {
      final String[] nodes = new String[length];
      Arrays.fill(nodes, ONE_LEVEL_PATH_WILDCARD);
      System.arraycopy(prefix, 0, nodes, 0, prefix.length);
      final ExtendedPartialPath partialPath = new ExtendedPartialPath(nodes, isRestrict);
      for (final SchemaFilter schemaFilter : tagFilterList) {
        if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.TAG)) {
          final int index = ((TagFilter) schemaFilter).getIndex() + prefix.length;
          final SchemaFilter childFilter = ((TagFilter) schemaFilter).getChild();
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
