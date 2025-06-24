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

import org.apache.iotdb.db.exception.sql.SemanticException;
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
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer.CLASSIFIER;
import static org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern.PhysicalValuePointer.MATCH_NUMBER;
import static org.apache.tsfile.enums.TSDataType.STRING;

public class PatternExpressionComputation {
  // Each valueAccessor points to the data in a specific row and column of the actual TsBlock, and
  // then provides this data as a parameter to the `computation`.
  private final List<PhysicalValueAccessor> valueAccessors;

  // It stores the computation logic of parameterized expressions. The parts of the expression that
  // depend on actual data in the TsBlock are delegated to the valueAccessor for positioning.
  private final Computation computation;

  public PatternExpressionComputation(
      List<PhysicalValueAccessor> valueAccessors, Computation computation) {
    this.valueAccessors = valueAccessors;
    this.computation = computation;
  }

  public Object compute(
      int currentRow,
      ArrayView matchedLabels, // If the value is i, the currentRow matches labelNames[i]
      int partitionStart,
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
              values.add(getValueFromPartition(partition, pointer, position - partitionStart));
            }

          } else {
            values.add(null);
          }
        }
      }
    }

    return computation.evaluate(values);
  }

  /** output of empty match */
  public Object computeEmpty(long matchNumber) {
    List<Object> values = new ArrayList<>();

    for (PhysicalValueAccessor accessor : valueAccessors) {
      if (accessor instanceof PhysicalValuePointer) {
        PhysicalValuePointer pointer = (PhysicalValuePointer) accessor;
        int channel = pointer.getSourceChannel();
        if (channel == MATCH_NUMBER) {
          values.add(matchNumber);
        }
      }
    }

    if (!values.isEmpty()) {
      return matchNumber;
    } else {
      return null;
    }
  }

  private Object getValueFromPartition(
      Partition partition, PhysicalValuePointer pointer, int position) {
    int channel = pointer.getSourceChannel();
    Type type = pointer.getType();

    if (type instanceof BooleanType) {
      return partition.getBoolean(channel, position);
    } else if (type instanceof IntType) {
      return partition.getInt(channel, position);
    } else if (type instanceof LongType || type instanceof TimestampType) {
      return partition.getLong(channel, position);
    } else if (type instanceof FloatType) {
      return partition.getFloat(channel, position);
    } else if (type instanceof DoubleType) {
      return partition.getDouble(channel, position);
    } else if (type instanceof StringType) {
      return partition.getBinary(channel, position);
    } else {
      throw new SemanticException("Unsupported type: " + type.getClass().getSimpleName());
    }
  }
}
