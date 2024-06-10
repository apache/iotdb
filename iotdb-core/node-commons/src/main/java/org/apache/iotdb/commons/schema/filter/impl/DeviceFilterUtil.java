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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;

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
      String database,
      String tableName,
      int idColumnNum,
      List<List<SchemaFilter>> idDeterminedFilterList) {
    List<PartialPath> pathList = new ArrayList<>();
    int length = idColumnNum + 3;
    for (List<SchemaFilter> idFilterList : idDeterminedFilterList) {
      String[] nodes = new String[length];
      Arrays.fill(nodes, ONE_LEVEL_PATH_WILDCARD);
      nodes[0] = PATH_ROOT;
      nodes[1] = database;
      nodes[2] = tableName;
      for (SchemaFilter schemaFilter : idFilterList) {
        if (schemaFilter.getSchemaFilterType().equals(SchemaFilterType.DEVICE_ID)) {
          DeviceIdFilter deviceIdFilter = (DeviceIdFilter) schemaFilter;
          nodes[deviceIdFilter.getIndex() + 3] = deviceIdFilter.getValue();
        } else {
          throw new IllegalStateException("Input single filter must be DeviceIdFilter");
        }
      }
      pathList.add(new PartialPath(nodes));
    }

    return pathList;
  }
}
