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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public interface GroupedAccumulator {

  long getEstimatedSize();

  void setGroupCount(long groupCount);

  void addInput(int[] groupIds, Column[] arguments);

  void addIntermediate(int[] groupIds, Column argument);

  void evaluateIntermediate(int groupId, ColumnBuilder columnBuilder);

  void evaluateFinal(int groupId, ColumnBuilder columnBuilder);

  void prepareFinal();

  void reset();
}
