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

package org.apache.tsfile.read.query.executor;

import org.apache.tsfile.exception.read.NoColumnException;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.read.UnsupportedOrderingException;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.query.executor.task.DeviceTaskIterator;
import org.apache.tsfile.read.reader.block.DeviceOrderedTsBlockReader;
import org.apache.tsfile.read.reader.block.TsBlockReader;
import org.apache.tsfile.read.reader.block.TsBlockReader.EmptyTsBlockReader;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableQueryExecutor {

  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;
  private TableQueryOrdering tableQueryOrdering;
  private int blockSize = 1024;

  public TableQueryExecutor(
      IMetadataQuerier metadataQuerier,
      IChunkLoader chunkLoader,
      TableQueryOrdering tableQueryOrdering) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
    this.tableQueryOrdering = tableQueryOrdering;
  }

  /**
   * @param tableName table to query
   * @param columns columns to query (ID or MEASUREMENT)
   * @param timeFilter time predicate
   * @param idFilter id predicate
   * @param measurementFilter measurement predicate
   * @return an iterator of TsBlocks
   * @throws ReadProcessException if the read process fails
   */
  public TsBlockReader query(
      String tableName,
      List<String> columns,
      ExpressionTree timeFilter,
      ExpressionTree idFilter,
      ExpressionTree measurementFilter)
      throws ReadProcessException {
    TsFileMetadata fileMetadata = metadataQuerier.getWholeFileMetadata();
    MetadataIndexNode tableRoot = fileMetadata.getTableMetadataIndexNode(tableName);
    TableSchema tableSchema = fileMetadata.getTableSchemaMap().get(tableName);
    if (tableRoot == null || tableSchema == null) {
      return new EmptyTsBlockReader();
    }

    ColumnMapping columnMapping = new ColumnMapping();
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      columnMapping.add(column, i, tableSchema);
    }
    columnMapping.add(measurementFilter);

    DeviceTaskIterator deviceTaskIterator =
        new DeviceTaskIterator(
            columns, tableRoot, columnMapping, metadataQuerier, idFilter, tableSchema);
    switch (tableQueryOrdering) {
      case DEVICE:
        return new DeviceOrderedTsBlockReader(
            deviceTaskIterator,
            metadataQuerier,
            chunkLoader,
            timeFilter,
            measurementFilter,
            blockSize);
      case TIME:
      default:
        throw new UnsupportedOrderingException(tableQueryOrdering.toString());
    }
  }

  public class ColumnMapping {
    /**
     * The same column may occur multiple times in a query, but we surely do not want to read it
     * redundantly. This mapping is used to put data of the same series into multiple columns.
     */
    private Map<String, List<Integer>> columnPosMap = new HashMap<>();

    private Set<String> idColumns = new HashSet<>();
    private Set<String> measurementColumns = new HashSet<>();

    public void add(String columnName, int i, TableSchema schema) throws NoColumnException {
      final int columnIndex = schema.findColumnIndex(columnName);
      if (columnIndex < 0) {
        throw new NoColumnException(columnName);
      }

      final ColumnCategory columnCategory = schema.getColumnTypes().get(columnIndex);
      columnPosMap.computeIfAbsent(columnName, k -> new ArrayList<>()).add(i);
      if (columnCategory.equals(ColumnCategory.ID)) {
        idColumns.add(columnName);
      } else {
        measurementColumns.add(columnName);
      }
    }

    public void add(ExpressionTree measurementFilter) {
      // TODO: get measurements in the filter and add them to measurementColumns
    }

    public List<Integer> getColumnPos(String columnName) {
      return columnPosMap.getOrDefault(columnName, Collections.emptyList());
    }

    public boolean isId(String columnName) {
      return idColumns.contains(columnName);
    }

    public boolean isMeasurement(String columnName) {
      return measurementColumns.contains(columnName);
    }

    public Set<String> getIdColumns() {
      return idColumns;
    }

    public Set<String> getMeasurementColumns() {
      return measurementColumns;
    }
  }

  public enum TableQueryOrdering {
    TIME,
    DEVICE
  }
}
