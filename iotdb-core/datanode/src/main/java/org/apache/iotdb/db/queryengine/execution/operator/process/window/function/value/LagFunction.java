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
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.List;

public class LagFunction extends ValueWindowFunction {
  private final int channel;
  private final int offsetChannel;
  private final int defaultValChannel;
  private final boolean ignoreNull;

  public LagFunction(List<Integer> argumentChannels, boolean ignoreNull) {
    this.channel = argumentChannels.get(0);
    this.offsetChannel = argumentChannels.size() > 1 ? argumentChannels.get(1) : -1;
    this.defaultValChannel = argumentChannels.size() > 2 ? argumentChannels.get(2) : -1;
    this.ignoreNull = ignoreNull;
  }

  @Override
  public void transform(
      Partition partition, ColumnBuilder builder, int index, int frameStart, int frameEnd) {
    if (offsetChannel >= 0 && partition.isNull(offsetChannel, index)) {
      builder.appendNull();
      return;
    }

    int offset = offsetChannel >= 0 ? partition.getInt(offsetChannel, index) : 1;

    int pos;
    if (ignoreNull) {
      int nonNullCount = 0;
      pos = index - 1;
      while (pos >= 0) {
        if (!partition.isNull(channel, pos)) {
          nonNullCount++;
          if (nonNullCount == offset) {
            break;
          }
        }

        pos--;
      }
    } else {
      pos = index - offset;
    }

    if (pos >= 0) {
      if (!partition.isNull(channel, pos)) {
        partition.writeTo(builder, channel, pos);
      } else {
        builder.appendNull();
      }
    } else if (defaultValChannel >= 0) {
      writeDefaultValue(partition, defaultValChannel, index, builder);
    } else {
      builder.appendNull();
    }
  }

  private void writeDefaultValue(
      Partition partition, int defaultValChannel, int index, ColumnBuilder builder) {
    TSDataType dataType = builder.getDataType();
    switch (dataType) {
      case INT32:
      case DATE:
        builder.writeInt(partition.getInt(defaultValChannel, index));
        return;
      case INT64:
      case TIMESTAMP:
        builder.writeLong(partition.getLong(defaultValChannel, index));
        return;
      case FLOAT:
        builder.writeFloat(partition.getFloat(defaultValChannel, index));
        return;
      case DOUBLE:
        builder.writeDouble(partition.getDouble(defaultValChannel, index));
        return;
      case BOOLEAN:
        builder.writeBoolean(partition.getBoolean(defaultValChannel, index));
        return;
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        builder.writeBinary(partition.getBinary(defaultValChannel, index));
        return;
      default:
        throw new UnSupportedDataTypeException(
            "Unsupported default value's data type in Lag: " + dataType);
    }
  }

  @Override
  public boolean needFrame() {
    return false;
  }
}
