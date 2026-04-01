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

package org.apache.iotdb.db.queryengine.transformation.dag.input;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.SafetyLine;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.SafetyLine.SafetyPile;
import org.apache.iotdb.db.queryengine.transformation.datastructure.iterator.RowListForwardIterator;
import org.apache.iotdb.db.queryengine.transformation.datastructure.row.ElasticSerializableRowList;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

public class QueryDataSetInputLayer {

  private TsBlockInputDataSet queryDataSet;
  private TSDataType[] dataTypes;

  private ElasticSerializableRowList rowList;
  private SafetyLine safetyLine;

  public QueryDataSetInputLayer(
      String queryId, float memoryBudgetInMB, TsBlockInputDataSet queryDataSet)
      throws QueryProcessException {
    construct(queryId, memoryBudgetInMB, queryDataSet);
  }

  private void construct(String queryId, float memoryBudgetInMB, TsBlockInputDataSet queryDataSet)
      throws QueryProcessException {
    this.queryDataSet = queryDataSet;
    dataTypes = queryDataSet.getDataTypes().toArray(new TSDataType[0]);
    rowList =
        new ElasticSerializableRowList(
            dataTypes, queryId, memoryBudgetInMB, 1 + dataTypes.length / 2);
    safetyLine = new SafetyLine();
  }

  public void updateRowRecordListEvictionUpperBound() {
    rowList.setEvictionUpperBound(safetyLine.getSafetyLine());
  }

  public BlockColumnReader constructValueReader(int columnIndex) {
    return new BlockColumnReader(columnIndex);
  }

  public BlockColumnReader constructTimeReader() {
    return new BlockColumnReader(dataTypes.length);
  }

  private class BlockColumnReader implements LayerReader {

    private final SafetyPile safetyPile = safetyLine.addSafetyPile();

    private final int columnIndex;
    private Column[] cachedColumns = null;
    private int cacheConsumed = 0;

    private final RowListForwardIterator iterator = rowList.constructIterator();

    BlockColumnReader(int columnIndex) {
      this.columnIndex = columnIndex;
    }

    @Override
    public final YieldableState yield() throws Exception {
      // Look up code-level cache
      if (cachedColumns != null && cacheConsumed < cachedColumns[0].getPositionCount()) {
        return YieldableState.YIELDABLE;
      }

      // Cache columns from row record list
      if (iterator.hasNext()) {
        iterator.next();
        cachedColumns = iterator.currentBlock();

        return YieldableState.YIELDABLE;
      }

      // Cache columns from child operator
      YieldableState yieldableState = queryDataSet.yield();
      if (YieldableState.YIELDABLE.equals(yieldableState)) {
        Column[] columns = queryDataSet.currentBlock();
        if (columns[0].getPositionCount() == 0) {
          cachedColumns = columns;
        } else {
          rowList.put(columns);
          iterator.next();
          cachedColumns = iterator.currentBlock();
        }
        // No need to call `.consume()` like method in queryDataSet
      }
      return yieldableState;
    }

    @Override
    public void consumedAll() {
      int steps = cachedColumns[0].getPositionCount() - cacheConsumed;
      safetyPile.moveForward(steps);

      cacheConsumed = 0;
      cachedColumns = null;
    }

    @Override
    public Column[] current() {
      Column valueColumn = cachedColumns[columnIndex];
      Column timeColumn = cachedColumns[cachedColumns.length - 1];
      return cacheConsumed == 0
          ? new Column[] {valueColumn, timeColumn}
          : new Column[] {
            valueColumn.subColumn(cacheConsumed), timeColumn.subColumn(cacheConsumed)
          };
    }

    @Override
    public TSDataType[] getDataTypes() {
      if (columnIndex == dataTypes.length) {
        return new TSDataType[] {TSDataType.INT64};
      }
      return new TSDataType[] {dataTypes[columnIndex]};
    }

    @Override
    public final boolean isConstantPointReader() {
      return false;
    }
  }
}
