/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.BitMap;

public interface Accumulator {

  /**
   * Column should be like: |Time | Value |
   *
   * <p>IgnoringNull is required when considering the row where the value of controlColumn is null
   *
   * <p>bitMap is required for group-by framework. When needed(eq. controlColumn is null), bitMap
   * can guide accumulator to skip some rows
   *
   * <p>lastIndex is required for group-by framework indicating the row to return to leave in
   * advance for various reasons(eq. current row doesn't satisfy the window)
   */
  void addInput(Column[] column, BitMap bitMap, int lastIndex);

  /**
   * For aggregation function like COUNT, SUM, partialResult should be single; But for AVG,
   * last_value, it should be double column with dictionary order.
   */
  void addIntermediate(Column[] partialResult);

  /**
   * This method can only be used in seriesAggregateScanOperator, it will use different statistics
   * based on the type of Accumulator.
   */
  void addStatistics(Statistics statistics);

  /**
   * Attention: setFinal should be invoked only once, and addInput() and addIntermediate() are not
   * allowed again.
   */
  void setFinal(Column finalResult);

  /**
   * For aggregation function like COUNT, SUM, partialResult should be single, so its output column
   * is single too; But for AVG(COUNT and SUM), LAST_VALUE(LAST_VALUE and MAX_TIME), the output
   * columns should be double in dictionary order.
   */
  void outputIntermediate(ColumnBuilder[] tsBlockBuilder);

  /** Final result is single column for any aggregation function. */
  void outputFinal(ColumnBuilder tsBlockBuilder);

  void reset();

  /**
   * This method can only be used in seriesAggregateScanOperator. For first_value or last_value in
   * decreasing order, we can get final result by the first record.
   */
  boolean hasFinalResult();

  TSDataType[] getIntermediateType();

  TSDataType getFinalType();
}
