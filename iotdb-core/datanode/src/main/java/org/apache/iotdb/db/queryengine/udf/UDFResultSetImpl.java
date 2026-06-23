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

package org.apache.iotdb.db.queryengine.udf;

import org.apache.iotdb.calc.execution.operator.source.relational.aggregation.RecordIterator;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.udf.api.UDFResultSet;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;

import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

/** Server-side implementation of {@link UDFResultSet}. */
public class UDFResultSetImpl implements UDFResultSet {

  private final InternalQueryResult queryResult;
  private final IoTDBLocalImpl owner;
  private final int index;

  private Iterator<Record> rowIterator;
  private final List<Type> columnTypes;
  private boolean closed;

  public UDFResultSetImpl(InternalQueryResult queryResult, IoTDBLocalImpl owner, int index) {
    this.queryResult = queryResult;
    this.owner = owner;
    this.index = index;
    this.columnTypes = buildColumnTypes(queryResult.getQueryExecution());
  }

  @Override
  public boolean hasNext() throws UDFException {
    ensureOpen();

    while (rowIterator == null || !rowIterator.hasNext()) {
      if (!queryResult.getQueryExecution().hasNextResult()) {
        return false;
      }
      Optional<TsBlock> batch;
      try {
        batch = queryResult.getQueryExecution().getBatchResult();
      } catch (IoTDBException e) {
        throw new UDFException(e.getMessage());
      }
      if (!batch.isPresent()) {
        return false;
      }
      TsBlock currentBlock = batch.get();
      rowIterator =
          new RecordIterator(
              Arrays.asList(currentBlock.getValueColumns()),
              columnTypes,
              currentBlock.getPositionCount());
    }
    return true;
  }

  @Override
  public Record next() throws UDFException {
    ensureOpen();

    if (rowIterator == null || !rowIterator.hasNext()) {
      throw new NoSuchElementException();
    }
    return rowIterator.next();
  }

  @Override
  public void close() throws UDFException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      queryResult.close();
    } catch (RuntimeException e) {
      throw new UDFException("Failed to close internal query result", e);
    }
    owner.markResultSetClosed(index);
  }

  private void ensureOpen() throws UDFException {
    if (closed) {
      throw new UDFException("UDFResultSet is already closed");
    }
  }

  private static List<Type> buildColumnTypes(IQueryExecution queryExecution) {
    return queryExecution.getDatasetHeader().getColumnHeaders().stream()
        .map(ColumnHeader::getColumnType)
        .map(UDFDataTypeTransformer::transformToUDFDataType)
        .map(UDFDataTypeTransformer::transformUDFDataTypeToReadType)
        .collect(Collectors.toList());
  }
}
