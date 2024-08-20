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

import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.IDeviceSchemaInfo;

import org.apache.tsfile.enums.TSDataType;

import java.util.Iterator;
import java.util.List;

public class DevicePredicateFilter extends DevicePredicateHandler
    implements Iterator<IDeviceSchemaInfo> {
  private int curIndex = 0;

  public DevicePredicateFilter(
      final List<TSDataType> filterOutputDataTypes,
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer,
      final List<ColumnTransformer> commonTransformerList,
      final String database,
      final String tableName,
      final List<ColumnHeader> columnHeaderList) {
    super(
        filterOutputDataTypes,
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        commonTransformerList,
        database,
        tableName,
        columnHeaderList);
  }

  @Override
  public boolean hasNext() {
    final boolean result = curIndex < indexes.size();
    if (!result && !deviceSchemaBatch.isEmpty()) {
      clear();
    }
    return result;
  }

  @Override
  public IDeviceSchemaInfo next() {
    return deviceSchemaBatch.get(indexes.get(curIndex++));
  }
}
