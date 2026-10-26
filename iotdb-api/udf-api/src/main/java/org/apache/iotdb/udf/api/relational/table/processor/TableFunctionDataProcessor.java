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

import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.TableFunctionAnalysis;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.List;

/** Each instance of TableFunctionDataProcessor processes one partition of data. */
public interface TableFunctionDataProcessor {

  default void beforeStart() {
    // do nothing
  }

  /** Same as {@link #beforeStart()} with access to {@link IoTDBLocal} for embedded queries. */
  default void beforeStart(IoTDBLocal local) {
    beforeStart();
  }

  /**
   * This method processes a portion of data. It is called multiple times until the partition is
   * fully processed.
   *
   * @param input {@link Record} including a portion of one partition for each table function's
   *     input table. A Record consists of columns requested during analysis (see {@link
   *     TableFunctionAnalysis#getRequiredColumns}).
   * @param properColumnBuilders A list of {@link ColumnBuilder} for each column in the output
   *     table.
   * @param passThroughIndexBuilder A {@link ColumnBuilder} for pass through columns. Index is
   *     started from 0 of the whole partition. It will be null if table argument is not declared
   *     with PASS_THROUGH_COLUMNS.
   */
  void process(
      Record input,
      List<ColumnBuilder> properColumnBuilders,
      ColumnBuilder passThroughIndexBuilder);

  /**
   * Same as {@link #process(Record, List, ColumnBuilder)} with access to {@link IoTDBLocal} for
   * embedded queries.
   */
  default void process(
      Record input,
      List<ColumnBuilder> properColumnBuilders,
      ColumnBuilder passThroughIndexBuilder,
      IoTDBLocal local) {
    process(input, properColumnBuilders, passThroughIndexBuilder);
  }

  /**
   * This method is called after all data is processed. It is used to finalize the output table and
   * close resource. All remaining data should be written to the columnBuilders.
   *
   * @param properColumnBuilders A list of {@link ColumnBuilder} for each column in the output
   *     table.
   * @param passThroughIndexBuilder A {@link ColumnBuilder} for pass through columns. Index is
   *     started from 0 of the whole partition. It will be null if table argument is not declared
   *     with PASS_THROUGH_COLUMNS.
   */
  default void finish(
      List<ColumnBuilder> properColumnBuilders, ColumnBuilder passThroughIndexBuilder) {
    // do nothing
  }

  /**
   * Same as {@link #finish(List, ColumnBuilder)} with access to {@link IoTDBLocal} for embedded
   * queries.
   */
  default void finish(
      List<ColumnBuilder> properColumnBuilders,
      ColumnBuilder passThroughIndexBuilder,
      IoTDBLocal local) {
    finish(properColumnBuilders, passThroughIndexBuilder);
  }

  /** This method is mainly used to release the resources used in the UDF. */
  default void beforeDestroy() {
    // do nothing
  }

  /** Same as {@link #beforeDestroy()} with access to {@link IoTDBLocal}. */
  default void beforeDestroy(IoTDBLocal local) {
    beforeDestroy();
  }
}
