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
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.udf.api.UDFResultSet;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.relational.access.Record;

import org.apache.tsfile.block.column.Column;
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

  private final List<UDFResultSetImpl> openResultSets;
  private final int index;
  private final InternalQueryResult queryResult;
  private final List<Type> columnTypes;

  /** null when output columns align with TsBlock value columns without index mapping */
  private final int[] columnIndexes;

  private Iterator<Record> rowIterator;
  private boolean closed;

  public UDFResultSetImpl(
      List<UDFResultSetImpl> openResultSets, int index, InternalQueryResult queryResult) {
    this.openResultSets = openResultSets;
    this.index = index;
    this.queryResult = queryResult;
    DatasetHeader datasetHeader = queryResult.getDatasetHeader();
    this.columnTypes = buildColumnTypes(datasetHeader.getColumnHeaders());
    this.columnIndexes = buildColumnIndexes(datasetHeader.getColumnIndex2TsBlockColumnIndexList());
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
        throw new UDFException(e.getMessage(), e);
      }
      if (!batch.isPresent()) {
        return false;
      }
      TsBlock currentBlock = batch.get();
      if (currentBlock.getPositionCount() == 0) {
        continue;
      }
      rowIterator =
          new RecordIterator(
              extractColumns(currentBlock), columnTypes, currentBlock.getPositionCount());
    }
    return true;
  }

  @Override
  public Record next() throws UDFException {
    if (!hasNext()) {
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
    openResultSets.set(index, null);
    try {
      queryResult.close();
    } catch (RuntimeException e) {
      throw new UDFException(
          DataNodeQueryMessages.EXCEPTION_FAILED_TO_CLOSE_INTERNAL_QUERY_RESULT_ED86BF5E, e);
    }
  }

  private void ensureOpen() throws UDFException {
    if (closed) {
      throw new UDFException(
          DataNodeQueryMessages.EXCEPTION_UDFRESULTSET_IS_ALREADY_CLOSED_E1ACABE3);
    }
  }

  private static List<Type> buildColumnTypes(List<ColumnHeader> columnHeaders) {
    return columnHeaders.stream()
        .map(ColumnHeader::getColumnType)
        .map(UDFDataTypeTransformer::transformToUDFDataType)
        .map(UDFDataTypeTransformer::transformUDFDataTypeToReadType)
        .collect(Collectors.toList());
  }

  private static int[] buildColumnIndexes(List<Integer> columnIndex2TsBlockColumnIndexList) {
    if (columnIndex2TsBlockColumnIndexList == null
        || columnIndex2TsBlockColumnIndexList.isEmpty()) {
      return null;
    }
    int size = columnIndex2TsBlockColumnIndexList.size();
    int[] indexes = new int[size];
    for (int i = 0; i < size; i++) {
      indexes[i] = columnIndex2TsBlockColumnIndexList.get(i);
    }
    return indexes;
  }

  private List<Column> extractColumns(TsBlock tsBlock) {
    Column[] valueColumns = tsBlock.getValueColumns();
    if (valueColumns.length == 0) {
      return Arrays.asList(valueColumns);
    }

    if (columnIndexes != null) {
      return Arrays.asList(tsBlock.getColumns(columnIndexes));
    }
    return Arrays.asList(valueColumns);
  }
}
