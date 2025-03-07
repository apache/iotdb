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

package org.apache.iotdb.udf.api.relational.table;

import org.apache.iotdb.udf.api.relational.table.argument.DescribedSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * An object of this class is produced by the `analyze()` method of a `TableFunction`
 * implementation. It contains all the analysis results:
 */
public class TableFunctionAnalysis {

  /**
   * The `properColumnSchema` field is used to inform the Analyzer of the proper columns returned by
   * the Table Function, that is, the columns produced by the function.
   */
  private final Optional<DescribedSchema> properColumnSchema;

  /**
   * The `requiredColumns` field is used to inform the Analyzer of the columns from the table
   * arguments that are necessary to execute the table function.
   */
  // a map from table argument name to list of column indexes for all columns required from the
  // table argument
  private final Map<String, List<Integer>> requiredColumns;

  private TableFunctionAnalysis(
      Optional<DescribedSchema> properColumnSchema, Map<String, List<Integer>> requiredColumns) {
    this.properColumnSchema = requireNonNull(properColumnSchema, "returnedType is null");
    this.requiredColumns = requiredColumns;
  }

  public Optional<DescribedSchema> getProperColumnSchema() {
    return properColumnSchema;
  }

  public Map<String, List<Integer>> getRequiredColumns() {
    return requiredColumns;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private DescribedSchema properColumnSchema;
    private final Map<String, List<Integer>> requiredColumns = new HashMap<>();

    private Builder() {}

    public Builder properColumnSchema(DescribedSchema properColumnSchema) {
      this.properColumnSchema = properColumnSchema;
      return this;
    }

    public Builder requiredColumns(String tableArgument, List<Integer> columns) {
      this.requiredColumns.put(tableArgument, columns);
      return this;
    }

    public TableFunctionAnalysis build() {
      return new TableFunctionAnalysis(Optional.ofNullable(properColumnSchema), requiredColumns);
    }
  }
}
