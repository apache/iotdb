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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.schema.table.InformationSchema;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.BytesUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class InformationSchemaContentSupplierFactory {
  private InformationSchemaContentSupplierFactory() {}

  public static Iterator<TsBlock> getSupplier(
      final String tableName, final List<TSDataType> dataTypes) {
    if (tableName.equals(InformationSchema.QUERIES)) {
      return new QueriesSupplier(dataTypes);
    } else {
      throw new UnsupportedOperationException("Unknown table: " + tableName);
    }
  }

  private static class QueriesSupplier extends TsBlockSupplier {
    private final long currTime = System.currentTimeMillis();
    private final List<IQueryExecution> queryExecutions =
        Coordinator.getInstance().getAllQueryExecutions();

    private QueriesSupplier(final List<TSDataType> dataTypes) {
      super(dataTypes, Coordinator.getInstance().getAllQueryExecutions().size());
    }

    @Override
    protected void constructLine() {
      IQueryExecution queryExecution = queryExecutions.get(nextConsumedIndex);

      if (queryExecution.getSQLDialect().equals(IClientSession.SqlDialect.TABLE)) {
        String[] splits = queryExecution.getQueryId().split("_");
        int dataNodeId = Integer.parseInt(splits[splits.length - 1]);

        columnBuilders[0].writeBinary(BytesUtils.valueOf(queryExecution.getQueryId()));
        columnBuilders[1].writeLong(queryExecution.getStartExecutionTime());
        columnBuilders[2].writeInt(dataNodeId);
        columnBuilders[3].writeFloat(
            (float) (currTime - queryExecution.getStartExecutionTime()) / 1000);
        columnBuilders[4].writeBinary(
            BytesUtils.valueOf(queryExecution.getExecuteSQL().orElse("UNKNOWN")));
        resultBuilder.declarePosition();
      }
    }
  }

  private abstract static class TsBlockSupplier implements Iterator<TsBlock> {

    protected final TsBlockBuilder resultBuilder;
    protected final ColumnBuilder[] columnBuilders;
    private final int totalSize;
    protected int nextConsumedIndex;

    private TsBlockSupplier(final List<TSDataType> dataTypes, final int totalSize) {
      this.resultBuilder = new TsBlockBuilder(dataTypes);
      this.columnBuilders = resultBuilder.getValueColumnBuilders();
      this.totalSize = totalSize;
    }

    @Override
    public boolean hasNext() {
      return nextConsumedIndex < totalSize;
    }

    @Override
    public TsBlock next() {
      if (nextConsumedIndex >= totalSize) {
        throw new NoSuchElementException();
      }
      while (nextConsumedIndex < totalSize && !resultBuilder.isFull()) {
        constructLine();
        nextConsumedIndex++;
      }
      TsBlock result =
          resultBuilder.build(
              new RunLengthEncodedColumn(
                  TableScanOperator.TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
      resultBuilder.reset();
      return result;
    }

    protected abstract void constructLine();
  }
}
