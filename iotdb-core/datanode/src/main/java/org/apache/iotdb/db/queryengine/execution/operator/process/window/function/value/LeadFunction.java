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

public class LeadFunction extends ValueWindowFunction {
  private final int channel;
  private final Integer offset;
  private final Integer defaultVal;
  private final boolean ignoreNull;

  public LeadFunction(int channel, Integer offset, Integer defaultVal, boolean ignoreNull) {
    this.channel = channel;
    this.offset = offset == null ? 1 : offset;
    this.defaultVal = defaultVal;
    this.ignoreNull = ignoreNull;
  }

  @Override
  public void transform(
      Partition partition, ColumnBuilder builder, int index, int frameStart, int frameEnd) {
    int length = partition.getPositionCount();

    int pos;
    if (ignoreNull) {
      int nonNullCount = 0;
      pos = index + 1;
      while (pos < length) {
        if (!partition.isNull(channel, pos)) {
          nonNullCount++;
          if (nonNullCount == offset) {
            break;
          }
        }

        pos++;
      }
    } else {
      pos = index + offset;
    }

    if (pos < length) {
      if (!partition.isNull(channel, pos)) {
        partition.writeTo(builder, channel, pos);
      } else {
        builder.appendNull();
      }
    } else if (defaultVal != null) {
      builder.writeObject(defaultVal);
    } else {
      builder.appendNull();
    }
  }

  @Override
  public boolean needFrame() {
    return false;
  }
}
