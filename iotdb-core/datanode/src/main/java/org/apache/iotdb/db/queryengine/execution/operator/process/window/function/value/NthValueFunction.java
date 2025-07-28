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

package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;

import org.apache.tsfile.block.column.ColumnBuilder;

import java.util.List;

public class NthValueFunction extends ValueWindowFunction {
  private final int valueChannel;
  private final int nChannel;
  private final boolean ignoreNull;

  public NthValueFunction(List<Integer> argumentChannels, boolean ignoreNull) {
    this.valueChannel = argumentChannels.get(0);
    this.nChannel = argumentChannels.get(1);
    this.ignoreNull = ignoreNull;
  }

  @Override
  public void transform(
      Partition partition, ColumnBuilder builder, int index, int frameStart, int frameEnd) {
    // Empty frame
    if (frameStart < 0 || partition.isNull(nChannel, index)) {
      builder.appendNull();
      return;
    }

    int pos;
    int n = partition.getInt(nChannel, index);
    if (ignoreNull) {
      // Handle nulls
      pos = frameStart;
      int nonNullCount = 0;
      while (pos <= frameEnd) {
        if (!partition.isNull(valueChannel, pos)) {
          nonNullCount++;
          if (nonNullCount == n) {
            break;
          }
        }
        pos++;
      }

      if (pos <= frameEnd) {
        partition.writeTo(builder, valueChannel, pos);
      } else {
        builder.appendNull();
      }
      return;
    }

    // n starts with 1
    pos = frameStart + n - 1;
    if (pos <= frameEnd) {
      if (!partition.isNull(valueChannel, pos)) {
        partition.writeTo(builder, valueChannel, pos);
      } else {
        builder.appendNull();
      }
    } else {
      builder.appendNull();
    }
  }
}
