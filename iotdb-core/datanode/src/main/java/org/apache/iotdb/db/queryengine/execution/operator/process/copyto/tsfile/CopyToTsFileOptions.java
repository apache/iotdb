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

package org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.CopyToOptions;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.RelationPlan;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.utils.constant.SqlConstant;

import org.apache.tsfile.enums.TSDataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CopyToTsFileOptions implements CopyToOptions {

  private static final String DEFAULT_TABLE_NAME = "default";

  private String targetTableName;
  private String targetTimeColumn;
  private Set<String> targetTagColumns;
  private final long targetMemoryThreshold;

  private boolean generateNewTimeColumn = false;

  public CopyToTsFileOptions(
      String targetTableName,
      String targetTimeColumn,
      Set<String> targetTagColumns,
      long targetMemoryThreshold) {
    this.targetTableName = targetTableName;
    this.targetTimeColumn = targetTimeColumn;
    this.targetTagColumns = targetTagColumns;
    this.targetMemoryThreshold = targetMemoryThreshold;
  }

  public Format getFormat() {
    return Format.TSFILE;
  }

  public boolean isGenerateNewTimeColumn() {
    return generateNewTimeColumn;
  }

  @Override
  public void infer(
      Analysis analysis, RelationPlan queryRelationPlan, List<ColumnHeader> columnHeaders) {
    List<Identifier> tables = queryRelationPlan.getScope().getTables();
    TsTable onlyOneQueriedTable = null;
    if (tables != null && tables.size() == 1) {
      onlyOneQueriedTable =
          DataNodeTableCache.getInstance()
              .getTable(analysis.getDatabaseName(), tables.get(0).toString(), false);
    }
    if (targetTableName == null) {
      targetTableName =
          onlyOneQueriedTable == null ? DEFAULT_TABLE_NAME : onlyOneQueriedTable.getTableName();
    }
    if (onlyOneQueriedTable != null) {
      if (targetTimeColumn == null || targetTagColumns == null) {
        inferTimeAndTags(onlyOneQueriedTable, columnHeaders);
      }
    }
    if (targetTimeColumn == null) {
      generateNewTimeColumn = true;
      targetTimeColumn = SqlConstant.TABLE_TIME_COLUMN_NAME;
    }
    if (targetTagColumns == null) {
      targetTagColumns = Collections.emptySet();
    }
  }

  private void inferTimeAndTags(TsTable tsTable, List<ColumnHeader> columnHeaders) {
    Map<String, ColumnHeader> columnName2ColumnHeaderMapInDataset = new HashMap<>();
    for (ColumnHeader columnHeader : columnHeaders) {
      columnName2ColumnHeaderMapInDataset.put(columnHeader.getColumnName(), columnHeader);
    }
    if (targetTagColumns == null) {
      boolean canMatchAllTags = true;
      List<TsTableColumnSchema> tagColumnsInTsTable = tsTable.getTagColumnSchemaList();
      for (TsTableColumnSchema tsTableColumnSchema : tagColumnsInTsTable) {
        String columnName = tsTableColumnSchema.getColumnName();
        ColumnHeader columnHeaderInDataset = columnName2ColumnHeaderMapInDataset.get(columnName);
        if (columnHeaderInDataset == null
            || columnHeaderInDataset.getColumnType() != tsTableColumnSchema.getDataType()) {
          canMatchAllTags = false;
          break;
        }
      }
      if (canMatchAllTags) {
        this.targetTagColumns = new LinkedHashSet<>(tagColumnsInTsTable.size());
        for (TsTableColumnSchema tagColumn : tagColumnsInTsTable) {
          targetTagColumns.add(tagColumn.getColumnName());
        }
      }
    }

    if (targetTimeColumn == null) {
      String timeColumnInTsTable = tsTable.getTimeColumnName();
      if (timeColumnInTsTable != null) {
        ColumnHeader timeColumnHeader =
            columnName2ColumnHeaderMapInDataset.get(timeColumnInTsTable);
        if (timeColumnHeader != null) {
          this.targetTimeColumn = timeColumnHeader.getColumnName();
        }
      }
      if (targetTimeColumn == null
          && columnName2ColumnHeaderMapInDataset.containsKey(SqlConstant.TABLE_TIME_COLUMN_NAME)) {
        this.targetTimeColumn = SqlConstant.TABLE_TIME_COLUMN_NAME;
      }
    }
  }

  private void inferTime() {}

  @Override
  public void check(List<ColumnHeader> columnHeaders) {
    if (generateNewTimeColumn && targetTagColumns.isEmpty()) {
      return;
    }
    Set<String> columns = new HashSet<>(targetTagColumns.size());
    int foundTagColumns = 0;
    for (ColumnHeader columnHeader : columnHeaders) {
      if (!generateNewTimeColumn
          && columnHeader.getColumnName().equals(targetTimeColumn)
          && columnHeader.getColumnType() != TSDataType.TIMESTAMP) {
        throw new SemanticException("Data type of target time column is not TIMESTAMP");
      }
      if (targetTagColumns.contains(columnHeader.getColumnName())) {
        if (columnHeader.getColumnType() != TSDataType.STRING) {
          throw new SemanticException(
              "Data type of tag column " + columnHeader.getColumnName() + " is not STRING");
        }
        foundTagColumns++;
      }
      columns.add(columnHeader.getColumnName());
    }
    if (columns.size() != columnHeaders.size()) {
      throw new SemanticException("Duplicate column names in query dataset.");
    }
    if (foundTagColumns != targetTagColumns.size()) {
      throw new SemanticException("Some specified tag columns are not exist in query dataset.");
    }
    if (foundTagColumns + (generateNewTimeColumn ? 0 : 1) == columns.size()) {
      throw new SemanticException("Number of field columns should be larger than 0.");
    }
  }

  public List<ColumnHeader> getRespColumnHeaders() {
    return ColumnHeaderConstant.COPY_TO_TSFILE_COLUMN_HEADERS;
  }

  @Override
  public long estimatedMaxRamBytesInWrite() {
    return targetMemoryThreshold;
  }

  public String getTargetTableName() {
    return targetTableName;
  }

  public String getTargetTimeColumn() {
    return targetTimeColumn;
  }

  public Set<String> getTargetTagColumns() {
    return targetTagColumns;
  }

  public long getTargetMemoryThreshold() {
    return targetMemoryThreshold;
  }
}
