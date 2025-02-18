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

package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.expression;

import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValueAccessor;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer;
import org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.matcher.ArrayView;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.BooleanType;
import org.apache.tsfile.read.common.type.DoubleType;
import org.apache.tsfile.read.common.type.FloatType;
import org.apache.tsfile.read.common.type.IntType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer.CLASSIFIER;
import static org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer.MATCH_NUMBER;
import static org.apache.tsfile.enums.TSDataType.STRING;

// TODO: 2.17 get 方法还未实现
// 专门用于计算 RPR 中的表达式
public class PatternExpressionComputation {

  // 访问 Partition 中的一组数据。一个访问器访问一个数据。
  // 如 B.value，访问当前行。
  // 如 LAST(B.value)，访问上一行。
  private final List<PhysicalValueAccessor> valueAccessors;

  // 计算逻辑：用于表示多个访问器对应的数据之间应该如何计算。其实就是一个传统的表达式了。
  // 如一共有两个参数，定义的运算可以为 #0 < #1
  // 如一共有三个参数，定义的运算可以为 #0 + #1 + #2
  // Computation 中存储计算逻辑。调用 evaluate 方法可以进行计算。
  // 参数的个数由 valueAccessors 的大小决定。
  private final Computation computation;

  public PatternExpressionComputation(
      List<PhysicalValueAccessor> valueAccessors, Computation computation) {
    this.valueAccessors = valueAccessors;
    this.computation = computation;
  }

  public Object compute(
      int currentRow,
      ArrayView matchedLabels, // If the value is i, the currentRow matches labelNames[i]
      int searchStart,
      int searchEnd,
      int patternStart,
      long matchNumber,
      List<String> labelNames,
      Partition partition) {
    List<Object> values = new ArrayList<>();

    for (PhysicalValueAccessor accessor : valueAccessors) {
      if (accessor instanceof PhysicalValuePointer) {
        PhysicalValuePointer pointer = (PhysicalValuePointer) accessor;
        int channel = pointer.getSourceChannel();
        if (channel == MATCH_NUMBER) {
          values.add(matchNumber);
        } else {
          int position =
              pointer
                  .getLogicalIndexNavigation()
                  .resolvePosition(currentRow, matchedLabels, searchStart, searchEnd, patternStart);

          if (position >= 0) {
            if (channel == CLASSIFIER) {
              TSDataType type = STRING;
              if (position < patternStart || position >= patternStart + matchedLabels.length()) {
                // position out of match. classifier() function returns null.
                values.add(null);
              } else {
                // position within match. get the assigned label from matchedLabels.
                // note: when computing measures, all labels of the match can be accessed (even
                // those exceeding the current running position), both in RUNNING and FINAL
                // semantics
                values.add(labelNames.get(matchedLabels.get(position - patternStart)));
              }
            } else {
              // need to get the data from the partition according to the position
              values.add(getValueFromPartition(partition, pointer, position));
            }

          } else {
            values.add(null);
          }
        }
      }
    }

    return computation.evaluate(values);
  }

  private Object getValueFromPartition(
      Partition partition, PhysicalValuePointer pointer, int position) {
    int channel = pointer.getSourceChannel();
    Type type = pointer.getType();

    if (type instanceof BooleanType) {
      return partition.getBoolean(channel, position);
    } else if (type instanceof IntType) {
      return partition.getInt(channel, position);
    } else if (type instanceof LongType) {
      return partition.getLong(channel, position);
    } else if (type instanceof FloatType) {
      return partition.getFloat(channel, position);
    } else if (type instanceof DoubleType) {
      return partition.getDouble(channel, position);
    } else {
      throw new IllegalArgumentException("Unsupported type: " + type.getClass().getSimpleName());
    }
  }
}
