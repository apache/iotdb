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
package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.UpdateMemory;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;

import java.util.List;

public interface GroupByHash {
  static GroupByHash createGroupByHash(
      List<Type> types, boolean hasPrecomputedHash, int expectedSize, UpdateMemory updateMemory) {
    /*if (types.size() == 1 && types.get(0).equals(BIGINT)) {
        return new BigintGroupByHash(hasPrecomputedHash, expectedSize, updateMemory);
    }*/
    return new FlatGroupByHash(types, hasPrecomputedHash, expectedSize, updateMemory);
  }

  long getEstimatedSize();

  int getGroupCount();

  void appendValuesTo(int groupId, TsBlockBuilder pageBuilder);

  void addPage(Column[] groupedColumns);

  /**
   * The order of new group ids need to be the same as the order of incoming rows, i.e. new group
   * ids should be assigned in rows iteration order Example: rows: A B C B D A E group ids: 1 2 3 2
   * 4 1 5
   */
  int[] getGroupIds(Column[] groupedColumns);

  long getRawHash(int groupId);

  @VisibleForTesting
  int getCapacity();
}
