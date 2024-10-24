/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.file.metadata.statistics.Statistics;

public interface TableAccumulator {
  long getEstimatedSize();

  TableAccumulator copy();

  void addInput(Column[] arguments);

  void addIntermediate(Column argument);

  void evaluateIntermediate(ColumnBuilder columnBuilder);

  void evaluateFinal(ColumnBuilder columnBuilder);

  /**
   * This method can only be used in AggTableScan. For first/first_by or last/last_by in decreasing
   * order, we can get final result by the first record.
   */
  boolean hasFinalResult();

  /**
   * This method can only be used in AggTableScan, it will use different statistics based on the
   * type of Accumulator.
   */
  void addStatistics(Statistics[] statistics);

  void reset();
}
