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

import org.apache.iotdb.udf.api.relational.table.argument.Argument;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ArgumentsAnalysis {
  private final Map<String, Argument> passedArguments;
  private final List<TableArgumentAnalysis> tableArgumentAnalyses;

  public ArgumentsAnalysis(
      Map<String, Argument> passedArguments, List<TableArgumentAnalysis> tableArgumentAnalyses) {
    this.passedArguments =
        ImmutableMap.copyOf(requireNonNull(passedArguments, "passedArguments is null"));
    this.tableArgumentAnalyses =
        ImmutableList.copyOf(
            requireNonNull(tableArgumentAnalyses, "tableArgumentAnalyses is null"));
  }

  public Map<String, Argument> getPassedArguments() {
    return passedArguments;
  }

  public List<TableArgumentAnalysis> getTableArgumentAnalyses() {
    return tableArgumentAnalyses;
  }
}
