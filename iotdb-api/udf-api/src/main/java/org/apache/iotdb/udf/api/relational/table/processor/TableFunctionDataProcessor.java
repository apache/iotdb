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

package org.apache.iotdb.udf.api.relational.table.processor;

import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunction;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;
import org.apache.iotdb.udf.api.relational.access.Record;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.List;
import java.util.Optional;

/**
 * Each instance of TableFunctionDataProcessor processes one partition of data.
 */
public interface TableFunctionDataProcessor {

  /**
   * This method processes a portion of data. It is called multiple times until the partition is
   * fully processed.
   *
   * @param input {@link Record} including a portion of one partition for each table
   *     function's input table. Input is ordered according to the corresponding argument
   *     specifications in {@link TableFunction}. A page for an argument consists of columns
   *     requested during analysis (see {@link TableFunctionAnalysis#getRequiredColumns}).
   * @param columnBuilders a list of {@link ColumnBuilder} for each column in the output table.
   */
  void process(Record input, List<ColumnBuilder> columnBuilders);


  /**
   * This method is called after all data is processed. It is used to finalize the output table.
   * All remaining data should be written to the columnBuilders.
   * @param columnBuilders a list of {@link ColumnBuilder} for each column in the output table.
   */
  void finish(List<ColumnBuilder> columnBuilders);
}
