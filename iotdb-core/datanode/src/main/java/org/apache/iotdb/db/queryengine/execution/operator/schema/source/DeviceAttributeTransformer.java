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

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;
import java.util.Objects;

public class DeviceAttributeTransformer extends DevicePredicateFilter {
  private final List<LeafColumnTransformer> projectLeafColumnTransformerList;
  private final List<ColumnTransformer> projectOutputTransformers;

  public DeviceAttributeTransformer(
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer,
      final String database,
      final String tableName,
      final List<ColumnHeader> columnHeaderList,
      final List<LeafColumnTransformer> projectLeafColumnTransformerList,
      final List<ColumnTransformer> projectOutputTransformers) {
    super(
        filterLeafColumnTransformerList,
        filterOutputTransformer,
        database,
        tableName,
        columnHeaderList);
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformers = projectOutputTransformers;
  }

  public Object[] getTransformedObject(final IDeviceSchemaInfo deviceSchemaInfo) {
    final TsBlock block = match(deviceSchemaInfo);
    if (Objects.isNull(block)) {
      return new Object[0];
    }

    projectLeafColumnTransformerList.forEach(
        leafColumnTransformer -> leafColumnTransformer.initFromTsBlock(block));

    return projectOutputTransformers.stream()
        .map(
            columnTransformer -> {
              columnTransformer.tryEvaluate();
              return columnTransformer.getColumn().getObject(0);
            })
        .toArray(Object[]::new);
  }

  @Override
  public void close() {
    super.close();
    projectOutputTransformers.forEach(ColumnTransformer::close);
  }
}
