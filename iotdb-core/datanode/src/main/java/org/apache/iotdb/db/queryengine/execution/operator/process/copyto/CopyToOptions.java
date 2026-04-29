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

package org.apache.iotdb.db.queryengine.execution.operator.process.copyto;

import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile.CopyToTsFileOptions;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.RelationPlan;

import org.apache.tsfile.utils.Accountable;

import java.util.List;
import java.util.Set;

/**
 * Interface for COPY TO command options.
 *
 * <p>This interface defines the configuration for COPY TO operations, including target format,
 * target table name, column mappings, and memory management. Implementations provide
 * format-specific validation and inference logic.
 */
public interface CopyToOptions extends Accountable {

  /**
   * Infers and validates options based on query analysis and relation plan.
   *
   * <p>This method is called during analysis phase to fill in default values and validate
   * user-specified options against the query structure. For example, it can infer the target time
   * column from the query's time column if not explicitly specified.
   *
   * @param analysis the query analysis containing metadata about the query
   * @param queryRelationPlan the logical relation plan of the inner query
   * @param columnHeaders the column headers from the query result
   */
  void infer(Analysis analysis, RelationPlan queryRelationPlan, List<ColumnHeader> columnHeaders);

  /**
   * Validates the options against the actual column schema.
   *
   * <p>This method is called after planning to ensure the specified options (e.g., target columns,
   * tags) are valid for the given output schema.
   *
   * @param columnHeaders the column headers of the query result
   * @throws SemanticException if validation fails
   */
  void check(List<ColumnHeader> columnHeaders);

  /**
   * Returns the response column headers for the result set.
   *
   * <p>These headers describe the columns that will be returned to the client after the COPY TO
   * operation completes (e.g., file path, row count).
   *
   * @return list of column headers for the response
   */
  List<ColumnHeader> getRespColumnHeaders();

  /**
   * Returns the output symbols that represent the columns in the COPY TO result.
   *
   * @return list of symbols for the output columns
   */
  List<Symbol> getOutputSymbols();

  /**
   * Returns the output column names for the result.
   *
   * @return list of column names
   */
  List<String> getOutputColumnNames();

  /**
   * Returns the target output format.
   *
   * @return the format enum value
   */
  Format getFormat();

  /**
   * Estimates the maximum memory usage in bytes required for writing.
   *
   * <p>This is used for memory management and determining whether to flush data to disk.
   *
   * @return estimated maximum memory usage in bytes
   */
  long estimatedMaxRamBytesInWrite();

  /** Supported output formats for COPY TO command. */
  enum Format {
    /** TsFile format output. */
    TSFILE,
  }

  class Builder {
    private CopyToOptions.Format format = CopyToOptions.Format.TSFILE;
    private String targetTableName = null;
    private String targetTimeColumn = null;
    private Set<String> targetTagColumns = null;
    private long memoryThreshold = 32 * 1024 * 1024;

    public Builder withFormat(CopyToOptions.Format format) {
      this.format = format;
      return this;
    }

    public Builder withTargetTableName(String targetTableName) {
      this.targetTableName = targetTableName;
      return this;
    }

    public Builder withTargetTimeColumn(String targetTimeColumn) {
      this.targetTimeColumn = targetTimeColumn;
      return this;
    }

    public Builder withTargetTagColumns(Set<String> targetTagColumns) {
      this.targetTagColumns = targetTagColumns;
      return this;
    }

    public Builder withMemoryThreshold(long memoryThreshold) {
      if (memoryThreshold <= 0) {
        throw new SemanticException("The memory threshold must be greater than 0.");
      }
      this.memoryThreshold = memoryThreshold;
      return this;
    }

    public CopyToOptions build() {
      switch (format) {
        case TSFILE:
        default:
          return new CopyToTsFileOptions(
              targetTableName, targetTimeColumn, targetTagColumns, memoryThreshold);
      }
    }
  }
}
