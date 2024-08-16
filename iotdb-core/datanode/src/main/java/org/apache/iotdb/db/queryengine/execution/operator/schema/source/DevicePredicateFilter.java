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

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;

public class DevicePredicateFilter {
  private final List<LeafColumnTransformer> filterLeafColumnTransformerList;
  private final ColumnTransformer filterOutputTransformer;

  public DevicePredicateFilter(
      final List<LeafColumnTransformer> filterLeafColumnTransformerList,
      final ColumnTransformer filterOutputTransformer) {
    this.filterLeafColumnTransformerList = filterLeafColumnTransformerList;
    this.filterOutputTransformer = filterOutputTransformer;
  }

  // Single row tsBlock
  public boolean match(final TsBlock input) {
    // feed Filter ColumnTransformer, including TimeStampColumnTransformer and constant
    filterLeafColumnTransformerList.forEach(
        leafColumnTransformer -> leafColumnTransformer.initFromTsBlock(input));
    filterOutputTransformer.tryEvaluate();

    final Column filterColumn = filterOutputTransformer.getColumn();
    return !filterColumn.isNull(0) && filterColumn.getBoolean(0);
  }
}
