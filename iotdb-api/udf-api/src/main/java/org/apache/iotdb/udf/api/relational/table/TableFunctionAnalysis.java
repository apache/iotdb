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

import org.apache.iotdb.udf.api.relational.table.argument.Descriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * An object of this class is produced by the `analyze()` method of a `ConnectorTableFunction`
 * implementation. It contains all the analysis results:
 */
public class TableFunctionAnalysis {

  /**
   * The `returnedType` field is used to inform the Analyzer of the proper columns returned by the
   * Table Function, that is, the columns produced by the function, as opposed to the columns passed
   * from the input tables. The `returnedType` should only be set if the declared returned type is
   * GENERIC_TABLE.
   */
  private final Optional<Descriptor> returnedType;

  /**
   * The `requiredColumns` field is used to inform the Analyzer of the columns from the table
   * arguments that are necessary to execute the table function.
   */
  // a map from table argument name to list of column indexes for all columns required from the
  // table argument
  private final Map<String, List<Integer>> requiredColumns;

  private TableFunctionAnalysis(
      Optional<Descriptor> returnedType, Map<String, List<Integer>> requiredColumns) {
    this.returnedType = requireNonNull(returnedType, "returnedType is null");
    returnedType.ifPresent(
        descriptor -> {
          if (!descriptor.isTyped()) {
            throw new IllegalArgumentException("field types should be specified");
          }
        });
    this.requiredColumns = requiredColumns;
  }

  public Optional<Descriptor> getReturnedType() {
    return returnedType;
  }

  public Map<String, List<Integer>> getRequiredColumns() {
    return requiredColumns;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private Descriptor returnedType;
    private final Map<String, List<Integer>> requiredColumns = new HashMap<>();

    private Builder() {}

    public Builder returnedType(Descriptor returnedType) {
      this.returnedType = returnedType;
      return this;
    }

    public Builder requiredColumns(String tableArgument, List<Integer> columns) {
      this.requiredColumns.put(tableArgument, columns);
      return this;
    }

    public TableFunctionAnalysis build() {
      return new TableFunctionAnalysis(Optional.ofNullable(returnedType), requiredColumns);
    }
  }
}
