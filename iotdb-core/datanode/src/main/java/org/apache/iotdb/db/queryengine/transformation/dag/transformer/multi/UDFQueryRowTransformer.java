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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.multi;

import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;

public class UDFQueryRowTransformer extends UniversalUDFQueryTransformer {

  protected final LayerReader layerReader;

  public UDFQueryRowTransformer(LayerReader layerReader, UDTFExecutor executor) {
    super(executor);
    this.layerReader = layerReader;
  }

  @Override
  protected YieldableState tryExecuteUDFOnce() throws Exception {
    final YieldableState yieldableState = layerReader.yield();
    if (yieldableState != YieldableState.YIELDABLE) {
      return yieldableState;
    }

    Column[] columns = layerReader.current();
    cachedColumns = execute(columns);

    layerReader.consumedAll();
    return YieldableState.YIELDABLE;
  }

  private Column[] execute(Column[] columns) throws Exception {
    int count = columns[0].getPositionCount();
    TimeColumnBuilder timeColumnBuilder = new TimeColumnBuilder(null, count);
    ColumnBuilder valueColumnBuilder = TypeUtils.initColumnBuilder(tsDataType, count);

    executor.execute(columns, timeColumnBuilder, valueColumnBuilder);
    return executor.getCurrentBlock();
  }
}
