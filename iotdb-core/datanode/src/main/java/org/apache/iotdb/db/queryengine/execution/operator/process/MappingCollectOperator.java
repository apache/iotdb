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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

public class MappingCollectOperator extends CollectOperator {
  protected static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(MappingCollectOperator.class);

  // record mapping for each child
  private final List<List<Integer>> mappings;

  private final int outputColumnsCount;

  public MappingCollectOperator(
      OperatorContext operatorContext, List<Operator> children, List<List<Integer>> mappings) {
    super(operatorContext, children);
    this.mappings = mappings;
    outputColumnsCount = mappings.get(0).size();
  }

  @Override
  public TsBlock next() throws Exception {
    if (children.get(currentIndex).hasNextWithTimer()) {
      TsBlock tsBlock = children.get(currentIndex).nextWithTimer();
      if (tsBlock == null) {
        return null;
      } else {
        Column[] columns = new Column[outputColumnsCount];
        List<Integer> mapping = mappings.get(currentIndex);
        for (int i = 0; i < columns.length; i++) {
          columns[i] = tsBlock.getColumn(mapping.get(i));
        }
        return TsBlock.wrapBlocksWithoutCopy(
            tsBlock.getPositionCount(),
            new RunLengthEncodedColumn(
                TableScanOperator.TIME_COLUMN_TEMPLATE, tsBlock.getPositionCount()),
            columns);
      }
    } else {
      closeCurrentChild(currentIndex);
      currentIndex++;
      return null;
    }
  }

  protected void closeCurrentChild(int index) throws Exception {
    children.get(index).close();
    children.set(index, null);
    mappings.set(index, null);
  }
}
