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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer.tablefunction;

import org.apache.iotdb.udf.api.relational.table.TableFunctionHandle;
import org.apache.iotdb.udf.api.relational.table.argument.Argument;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;

public class TableFunctionInvocationAnalysis {
  private final String functionName;
  private final Map<String, Argument> passedArguments;
  private final TableFunctionHandle tableFunctionHandle;
  private final List<TableArgumentAnalysis> tableArgumentAnalyses;
  private final Map<String, List<Integer>> requiredColumns;
  private final int properColumnsCount;
  private final boolean requiredRecordSnapshot;

  public TableFunctionInvocationAnalysis(
      String name,
      Map<String, Argument> passedArguments,
      TableFunctionHandle tableFunctionHandle,
      ImmutableList<TableArgumentAnalysis> tableArgumentAnalyses,
      Map<String, List<Integer>> requiredColumns,
      int properColumnsCount,
      boolean requiredRecordSnapshot) {
    this.functionName = name;
    this.passedArguments = passedArguments;
    this.tableFunctionHandle = tableFunctionHandle;
    this.tableArgumentAnalyses = tableArgumentAnalyses;
    this.requiredColumns = requiredColumns;
    this.properColumnsCount = properColumnsCount;
    this.requiredRecordSnapshot = requiredRecordSnapshot;
  }

  public Map<String, List<Integer>> getRequiredColumns() {
    return requiredColumns;
  }

  public List<TableArgumentAnalysis> getTableArgumentAnalyses() {
    return tableArgumentAnalyses;
  }

  public String getFunctionName() {
    return functionName;
  }

  public Map<String, Argument> getPassedArguments() {
    return passedArguments;
  }

  public TableFunctionHandle getTableFunctionHandle() {
    return tableFunctionHandle;
  }

  public int getProperColumnsCount() {
    return properColumnsCount;
  }

  public boolean isRequiredRecordSnapshot() {
    return requiredRecordSnapshot;
  }
}
