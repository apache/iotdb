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

package org.apache.iotdb.commons.queryengine.plan.relational.function;

import org.apache.iotdb.udf.api.IoTDBLocal;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.relational.table.processor.TableFunctionDataProcessor;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.List;
import java.util.function.BooleanSupplier;

/** A built-in processor whose proper output can span blocks even for a single input row. */
public interface BatchedTableFunctionDataProcessor extends TableFunctionDataProcessor {

  /**
   * Accepts a row. The caller keeps the record valid until drainOutput returns true, before
   * advancing the input iterator or finishing the partition.
   */
  default void processInput(Record input, List<ColumnBuilder> builders, IoTDBLocal local) {
    process(input, builders, null, local);
  }

  /** Writes ready rows until the block is full. Returns true when more input can be accepted. */
  default boolean drainOutput(List<ColumnBuilder> builders, BooleanSupplier blockFull) {
    return true;
  }

  /**
   * Writes until the block is full or output is exhausted. Returns true when all rows are written.
   */
  boolean finishBatch(List<ColumnBuilder> builders, BooleanSupplier blockFull);

  @Override
  default void finish(List<ColumnBuilder> builders, ColumnBuilder passThroughIndexBuilder) {
    finishBatch(builders, () -> false);
  }
}
